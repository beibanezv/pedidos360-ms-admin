# pedidos360-ms-admin

Microservicio administrador de RabbitMQ y Kafka (Pedidos360). Sin base de datos:
gestiona RabbitMQ vía `RabbitAdmin` y Kafka vía `AdminClient`.

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
| DELETE | `/bindings` | Elimina el enlace con `cola`, `exchange` y `routingKey` → 204 |
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

Kafka exige JWT de Azure Entra válido con rol `Admin`. Para no romper la API
anterior, las rutas RabbitMQ conservan el control de acceso del Gateway;
no expongas públicamente el puerto de ms-admin.

## Kafka local (3 brokers y 3 ZooKeepers)

```powershell
cd docker/kafka-cluster
docker compose up -d
./comprobar.ps1 -EnVivo
# Opcional: detiene un broker, produce y lee en un tópico temporal, restaura el broker.
./comprobar.ps1 -EnVivo -ConCaida
```

Kafka UI de solo lectura en `http://127.0.0.1:8091`. Dos tópicos de negocio:
`p360.interacciones.v1` (7 días) y `p360.pagos.v1` (30 días). Ambos tienen
3 particiones, factor de replicación 3, `min.insync.replicas=2` y `cleanup.policy=delete`.
Usa `KAFKA_BOOTSTRAP_SERVERS` para conectar ms-admin; en local, 9092–9094.

| Método | Ruta bajo `/api/kafka` | Qué hace |
|---|---|---|
| GET | `/topics`, `/topics/{nombre}` | Lista tópicos o muestra configuración, líder, réplicas e ISR |
| POST | `/topics` | Crea un tópico; cuerpo: `nombre`, `particiones`, `replicas`, `retencionMs`, `limpieza` |
| PATCH | `/topics/{nombre}` | Cambia `retencionMs`, `limpieza` o aumenta `particiones` |
| DELETE | `/topics/{nombre}` | Elimina un tópico de prueba |
| GET | `/groups`, `/groups/{nombre}` | Lista grupos o muestra miembros, offsets y lag |
| DELETE | `/groups/{nombre}` | Elimina un grupo de prueba inactivo |

Los cambios solo permiten nombres `p360.demo.*`; no se toca el historial de
producción. Con el clúster local operativo, ejecuta `mvn test` con
`P360_TEST_KAFKA=true` para las pruebas de administración con broker real.
