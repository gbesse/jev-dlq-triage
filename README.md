# Jev Kafka Connect DLQ Triage

## Français

**But.** Regrouper des enregistrements de *dead letter queue* Kafka Connect et classer la famille probable de l'échec : `schema`, `transient`, `permission` ou `review`. Une décision Jev porte sur un groupe de même connecteur, étape et classe d'exception, avec les positions de cinq exemples au maximum. Aucun message n'est rejoué automatiquement.

**Entrée.** Un JSON `{"runbook_context":"...","records":[{"topic":"...","partition":0,"offset":1,"headers":{"__connect.errors.connector.name":"...","__connect.errors.stage":"...","__connect.errors.exception.class.name":"..."}}]}`. Les en-têtes peuvent aussi être une liste de paires `[clé,valeur]`. Activez `errors.deadletterqueue.context.headers.enable=true` dans Kafka Connect ; la [documentation Apache Kafka](https://kafka.apache.org/40/configuration/kafka-connect-configs/) précise le préfixe des en-têtes. Jusqu'à 500 messages et cinq groupes sont acceptés par dossier ; un dépassement est rejeté, jamais tronqué silencieusement.

**Données et usage.** `value`, `key`, les autres en-têtes et le texte libre de l'exception ne sont jamais envoyés à Jev. L'entrée JSON doit être préparée par votre consommateur ou export DLQ ; cette version ne se connecte pas directement à un broker. Installez `python3 -m pip install -r requirements.txt`, exécutez `python3 app.py --demo`, puis, avec `TYPESAFE_API_KEY` dans l'environnement, `python3 app.py --input chemin/vers/lot.json`. Chaque groupe entraîne un appel Jev ; tests : `python3 -m unittest discover -s tests -v`.

## English

**Purpose.** Group Kafka Connect dead letter queue records and classify the likely failure family: `schema`, `transient`, `permission`, or `review`. One Jev decision covers records sharing a connector, stage, and exception class, with positions for up to five samples. No message is replayed automatically.

**Input.** A JSON `{"runbook_context":"...","records":[{"topic":"...","partition":0,"offset":1,"headers":{"__connect.errors.connector.name":"...","__connect.errors.stage":"...","__connect.errors.exception.class.name":"..."}}]}`. Headers may also be `[key,value]` pairs. Enable `errors.deadletterqueue.context.headers.enable=true` in Kafka Connect; the [Apache Kafka docs](https://kafka.apache.org/40/configuration/kafka-connect-configs/) specify the header prefix. Up to 500 records and five groups are accepted per case; overflow is rejected, never silently truncated.

**Data and run.** `value`, `key`, other headers, and free-text exception messages are never sent to Jev. Prepare the input JSON with your DLQ consumer or export; this release does not connect directly to a broker. Install `python3 -m pip install -r requirements.txt`, run `python3 app.py --demo`, then, with `TYPESAFE_API_KEY` in the environment, run `python3 app.py --input path/to/batch.json`. Each group makes one Jev call; tests: `python3 -m unittest discover -s tests -v`.

## Español

**Objetivo.** Agrupar registros de la cola de mensajes fallidos de Kafka Connect y clasificar la familia probable del error: `schema`, `transient`, `permission` o `review`. Una decisión Jev cubre registros del mismo conector, etapa y clase de excepción, con posiciones de hasta cinco ejemplos. Ningún mensaje se vuelve a publicar automáticamente.

**Entrada.** Un JSON `{"runbook_context":"...","records":[{"topic":"...","partition":0,"offset":1,"headers":{"__connect.errors.connector.name":"...","__connect.errors.stage":"...","__connect.errors.exception.class.name":"..."}}]}`. Los encabezados también pueden ser pares `[clave,valor]`. Active `errors.deadletterqueue.context.headers.enable=true` en Kafka Connect; la [documentación de Apache Kafka](https://kafka.apache.org/40/configuration/kafka-connect-configs/) define el prefijo. Se aceptan hasta 500 registros y cinco grupos por expediente; el exceso se rechaza sin truncarlo silenciosamente.

**Datos y uso.** `value`, `key`, otros encabezados y mensajes de excepción de texto libre nunca se envían a Jev. Prepare el JSON con su consumidor o exportador DLQ; esta versión no se conecta directamente al broker. Instale `python3 -m pip install -r requirements.txt`, ejecute `python3 app.py --demo` y, con `TYPESAFE_API_KEY` en el entorno, `python3 app.py --input ruta/al/lote.json`. Cada grupo genera una llamada Jev; pruebas: `python3 -m unittest discover -s tests -v`.

MIT · v0.1.0
