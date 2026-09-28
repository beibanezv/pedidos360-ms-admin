# pedidos360-ms-admin

Microservicio administrador de RabbitMQ (Pedidos360). Sin base de datos:
gestiona colas, exchanges y bindings en tiempo de ejecución vía `RabbitAdmin`.

- Puerto: **8087**
- Base API: `http://localhost:8087/api/rabbitmq`

## Endpoints

| Método | Ruta | Qué hace |
|---|---|---|
| POST | `/queues` | Crea una cola (`{"nombre","durable?"}`) → 201 |
| POST | `/exchanges` | Crea un exchange (`{"nombre","tipo": direct\|topic\|fanout}`) → 201 |
| POST | `/bindings` | Enlaza cola→exchange (`{"cola","exchange","routingKey?"}`) → 201 |
| GET | `/queues/{nombre}` | Mensajes y consumidores de la cola → 200 |
| DELETE | `/queues/{nombre}` | Elimina la cola → 204 |
| DELETE | `/exchanges/{nombre}` | Elimina el exchange → 204 |
| POST | `/queues/{nombre}/purge` | Vacía la cola, devuelve cuántos eliminó → 200 |

Errores: 400 validación/tipo inválido, 404 cola o exchange inexistente,
503 broker no disponible.

## Clúster local (2 nodos)

El `docker-compose` del clúster vive en `docker/rabbitmq-cluster/`
(este es "el repo de RabbitMQ", así que el compose vive acá):

```powershell
cd docker/rabbitmq-cluster
docker compose up -d
```

UIs: http://localhost:15672 y http://localhost:15673 (admin/admin123
por defecto; se cambian con `RABBITMQ_USER`, `RABBITMQ_PASSWORD` y
`RABBITMQ_ERLANG_COOKIE`).

## Variables de entorno

| Variable | Default local |
|---|---|
| `RABBITMQ_HOST` | `localhost` (en AWS: IP privada de la EC2 `p360-rabbit`) |
| `RABBITMQ_PORT` | `5672` |
| `RABBITMQ_USER` / `RABBITMQ_PASSWORD` | `admin` / `admin123` |
| `RABBITMQ_VHOST` | `/` |

## Seguridad

Sin Spring Security a propósito: es una herramienta operativa interna.
En AWS lo protege el authorizer JWT del API Gateway (ruta `/admin/*`);
en local solo escucha en localhost.
