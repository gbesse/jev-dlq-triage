"""Group Kafka Connect DLQ error context without reading message values."""

import argparse
import json
import os
from pathlib import Path
import re
import sys

from jev import evaluate, normalize
from review_core import InputError, review

PREFIX = "__connect.errors."
SAFE_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]{1,200}")


def header_map(record):
    headers = record.get("headers")
    if isinstance(headers, list):
        if any(not isinstance(pair, list) or len(pair) != 2 for pair in headers):
            raise InputError("invalid_headers")
        headers = dict(headers)
    if not isinstance(headers, dict):
        raise InputError("invalid_headers")
    selected = {}
    for key in ("connector.name", "stage", "exception.class.name"):
        value = headers.get(PREFIX + key)
        if not isinstance(value, str) or not SAFE_IDENTIFIER.fullmatch(value):
            raise InputError("missing_error_context")
        selected[key] = value
    return selected


def adapt_groups(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get("records"), list):
        raise InputError("invalid_batch")
    records = payload["records"]
    if not 1 <= len(records) <= 500:
        raise InputError("invalid_record_count")
    context = payload.get("runbook_context", "")
    if not isinstance(context, str) or len(context) > 1000:
        raise InputError("invalid_context")
    groups = {}
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise InputError("invalid_record")
        header = header_map(record)
        topic, partition, offset = (
            record.get("topic"),
            record.get("partition"),
            record.get("offset"),
        )
        if (
            not isinstance(topic, str)
            or not 1 <= len(topic) <= 200
            or isinstance(partition, bool)
            or not isinstance(partition, int)
            or partition < 0
            or isinstance(offset, bool)
            or not isinstance(offset, int)
            or offset < 0
        ):
            raise InputError("invalid_position")
        key = tuple(header.values())
        group = groups.setdefault(key, {"count": 0, "positions": [], "evidence": []})
        group["count"] += 1
        if len(group["positions"]) < 5:
            group["positions"].append(
                {"topic": topic, "partition": partition, "offset": offset}
            )
            group["evidence"].append(f"records[{index}].headers")
    if len(groups) > 5:
        raise InputError("too_many_groups")
    cases = []
    for key, group in sorted(groups.items()):
        connector, stage, exception_class = key
        summary = {
            "connector": connector,
            "stage": stage,
            "exception_class": exception_class,
            "count": group["count"],
            "sample_positions": group["positions"],
        }
        text = (
            "Runbook context: "
            + context
            + "\nKafka Connect DLQ group (record values excluded): "
            + json.dumps(summary, sort_keys=True)
        )
        cases.append(
            {
                "subject": connector + "/" + stage + "/" + exception_class,
                "text": text,
                "evidence": group["evidence"],
                "flags": [],
            }
        )
    return cases


def main():
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--demo", action="store_true")
    mode.add_argument("--input", type=Path)
    args = parser.parse_args()
    try:
        with open("examples/policy.json", encoding="utf-8") as source:
            policy = json.load(source)
        if args.demo:
            with open("examples/case.json", encoding="utf-8") as source:
                payload = json.load(source)
            judge = lambda _: normalize("schema", 0.97, policy)
        else:
            if str(args.input) == "-":
                raw = sys.stdin.buffer.read(10_000_001)
                if len(raw) > 10_000_000:
                    raise InputError("input_too_large")
                payload = json.loads(raw)
            else:
                if args.input.stat().st_size > 10_000_000:
                    raise InputError("input_too_large")
                with args.input.open(encoding="utf-8") as source:
                    payload = json.load(source)
            key = os.environ.get("TYPESAFE_API_KEY")
            if not key:
                raise InputError("missing_key")
            judge = lambda text: evaluate(text, policy, key)
        cases = adapt_groups(payload)
        results = [review(case, policy, judge, lambda value: value) for case in cases]
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        code = str(exc) if isinstance(exc, InputError) else "invalid_input"
        parser.exit(2, code + "\n")
    except Exception:
        parser.exit(3, "decision_unavailable\n")
    print(
        json.dumps(
            {
                "demo": args.demo,
                "record_count": len(payload["records"]),
                "groups": results,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
