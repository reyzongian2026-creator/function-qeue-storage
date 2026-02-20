# Azure Function local + Queue Storage local + Temporal local + H2 local

Este proyecto te deja probar un flujo **muy parecido a Azure real**, pero 100% local y sin pagos:

1. Una Azure Function (Java) se dispara con mensajes en una Queue.
2. El mensaje trae `workflowId` (y opcional `runId`).
3. La Function consulta Temporal local (`localhost:7233`) para leer el historial del workflow.
4. Guarda los eventos en una base H2 local (`./data/workflow-history.mv.db`).

## Arquitectura local

- **Azure Function runtime local**: Azure Functions Core Tools.
- **Storage Queue local**: Azurite con `UseDevelopmentStorage=true`.
- **Temporal local**: tu servidor Temporal local.
- **Persistencia local**: H2 (archivo en disco).

---

## Manual paso a paso (compilar cola + function + producir workflowId)

> Este es el flujo mínimo recomendado en una máquina local.

### 0) Requisitos

- Java 17
- Maven 3.9+
- Azure Functions Core Tools v4
- Azure CLI (opcional, para enviar mensajes a cola)
- Azurite (npm o Docker)
- Temporal CLI (recomendado para crear workflows de prueba)

### 1) Configuración local

```bash
cp local.settings.sample.json local.settings.json
```

Variables importantes en `local.settings.json`:

- `QUEUE_NAME=workflow-requests`
- `AzureWebJobsStorage=UseDevelopmentStorage=true`
- `TEMPORAL_TARGET=localhost:7233`
- `TEMPORAL_NAMESPACE=default`
- `H2_JDBC_URL=jdbc:h2:file:./data/workflow-history;AUTO_SERVER=TRUE`

### 2) Arrancar la cola local (Azurite)

#### Opción Docker

```bash
docker run --rm --name azurite \
  -p 10000:10000 -p 10001:10001 -p 10002:10002 \
  mcr.microsoft.com/azure-storage/azurite
```

#### Crear la cola `workflow-requests`

```bash
az storage queue create \
  --name workflow-requests \
  --connection-string 'UseDevelopmentStorage=true'
```

### 3) Arrancar Temporal local

```bash
temporal server start-dev
```

### 4) ¿Cómo producir un `workflowId` real?

Tienes dos formas:

#### A) Definirlo tú manualmente (rápido)

Puedes usar un ID fijo, por ejemplo: `wf-demo-001`.

> Ojo: para que la Function encuentre historial real, ese workflow debe existir en Temporal.

#### B) Crearlo en Temporal (recomendado)

Lanza un workflow de prueba en Temporal y define tú mismo el ID:

```bash
temporal workflow start \
  --task-queue my-task-queue \
  --type YourWorkflowName \
  --workflow-id wf-demo-001
```

Luego valida que existe:

```bash
temporal workflow list --query "WorkflowId='wf-demo-001'"
```

Si tu workflow genera ejecución, ya tendrás historial para consultar.

### 5) Compilar proyecto

```bash
mvn clean package
```

### 6) Ejecutar la Function local

```bash
mvn -Pazure-functions azure-functions:run
```

### 7) Enviar `workflowId` a la cola

```bash
az storage message put \
  --queue-name workflow-requests \
  --content '{"workflowId":"wf-demo-001"}' \
  --connection-string 'UseDevelopmentStorage=true'
```

Si conoces `runId`, también puedes enviarlo:

```bash
az storage message put \
  --queue-name workflow-requests \
  --content '{"workflowId":"wf-demo-001","runId":"<run-id>"}' \
  --connection-string 'UseDevelopmentStorage=true'
```

### 8) ¿Qué debe pasar?

Cuando la Function consuma el mensaje:

1. Lee `workflowId` / `runId`.
2. Consulta Temporal en `TEMPORAL_TARGET`.
3. Trae historial paginado del workflow.
4. Inserta eventos en `workflow_history` (H2).

---

## Validar datos en H2

Puedes conectarte con H2 Console/JDBC al archivo:

- URL: `jdbc:h2:file:./data/workflow-history`
- User: `sa`
- Password: *(vacío por defecto)*

Consulta rápida:

```sql
SELECT workflow_id, run_id, event_id, event_type, event_time
FROM workflow_history
ORDER BY id DESC;
```

---

## Troubleshooting rápido

- Si la Function no se dispara:
  - revisa que `QUEUE_NAME` coincida con la cola creada.
  - revisa que Azurite esté activo y `UseDevelopmentStorage=true` funcione.
- Si no guarda en H2:
  - revisa logs de la Function para errores JDBC.
  - valida permisos de escritura de la carpeta `./data`.
- Si no encuentra historial de Temporal:
  - confirma que el `workflowId` realmente existe.
  - confirma `TEMPORAL_TARGET` y `TEMPORAL_NAMESPACE`.

---

## Estructura

- `src/main/java/com/example/functions/WorkflowHistoryFunction.java`: trigger de cola.
- `src/main/java/com/example/functions/TemporalHistoryClient.java`: cliente de historial Temporal.
- `src/main/java/com/example/functions/DatabaseRepository.java`: schema e inserción en H2.
- `src/main/java/com/example/functions/WorkflowRequest.java`: payload (`workflowId`, `runId`).
