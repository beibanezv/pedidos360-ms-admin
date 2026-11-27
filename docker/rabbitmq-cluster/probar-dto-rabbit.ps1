# Prueba REAL productor -> consumidor de DTOs espejo con RabbitMQ aislado.
# Contenedor, puerto, vhost, exchange y colas unicos: nada compartido.
# Uso:
#   powershell -ExecutionPolicy Bypass -File docker\rabbitmq-cluster\probar-dto-rabbit.ps1
# Requiere: docker, mvn -o con dependencias en ~/.m2, javac y los 6 repos compilados
# (mvn -o package -DskipTests; este script los compila si falta target/classes).

# Los comandos nativos escriben en stderr; se comprueba $LASTEXITCODE explicitamente.
$ErrorActionPreference = 'Continue'
Write-Host "COMANDO REPRODUCIBLE: powershell -ExecutionPolicy Bypass -File `"$($MyInvocation.MyCommand.Path)`""

$admin = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$base = Split-Path -Parent $admin
$repos = @(
  'pedidos360-ms-carrito', 'pedidos360-ms-productos', 'pedidos360-ms-orders',
  'pedidos360-ms-notificaciones', 'pedidos360-ms-auditoria', 'pedidos360-ms-cupones')

$sufijo = 'p360dto' + [guid]::NewGuid().ToString('N').Substring(0, 10)
$contenedor = $sufijo
$vhost = "/$sufijo"
$usuario = 'p360dto'
$clave = 'p360dto'
$listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
$listener.Start()
$puerto = $listener.LocalEndpoint.Port
$listener.Stop()
$tmp = Join-Path ([System.IO.Path]::GetTempPath()) $sufijo
$codigo = 1

Write-Host "BROKER aislado: contenedor=$contenedor puerto=127.0.0.1:$puerto vhost=$vhost imagen=rabbitmq:3.13-management-alpine"

try {
  New-Item -ItemType Directory -Path $tmp -Force | Out-Null

  # Claspath real: target/classes de ambos repos + dependencias Maven de cada repo.
  $vistas = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
  $entradas = [System.Collections.Generic.List[string]]::new()
  foreach ($repo in $repos) {
    $pom = Join-Path $base "$repo\pom.xml"
    $clases = Join-Path $base "$repo\target\classes"
    if (-not (Test-Path $clases)) {
      Write-Host "Compilando $repo (falta target/classes)..."
      & mvn -o -q -f $pom package -DskipTests
      if ($LASTEXITCODE -ne 0) { throw "No se pudo compilar $repo" }
    }
    [void]$entradas.Add($clases)
    $cpFile = Join-Path $tmp "$repo.cp"
    & mvn -o -q -f $pom dependency:build-classpath "-Dmdep.outputFile=$cpFile" -DincludeScope=test
    if ($LASTEXITCODE -ne 0) { throw "No se pudo construir el classpath de $repo" }
    foreach ($jar in (Get-Content -Raw -Encoding UTF8 $cpFile).Split(';')) {
      if ($jar.Trim().Length -gt 0 -and $vistas.Add($jar)) { [void]$entradas.Add($jar) }
    }
  }
  $cp = $entradas -join ';'
  Write-Host "CLASSPATH: $($repos.Count) repos target/classes + $($vistas.Count) jars (largo=$($cp.Length))"

  $codigoFuente = @'
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.MessageConverter;

/** Usa FQCN: los paquetes de los DTOs de cada microservicio NO se mezclan. */
public class ProbarDtoRabbit {
  static final String TYPE_ID = DefaultJackson2JavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME;
  static final String ORDEN = "11111111-1111-1111-1111-111111111111";
  static final String PRODUCTO = "22222222-2222-2222-2222-222222222222";
  static final String EVENTO = "33333333-3333-3333-3333-333333333333";
  static final String INTENTO = "44444444-4444-4444-4444-444444444444";
  static final Instant OCURRIO = Instant.parse("2026-11-27T10:15:30Z");

  static int puerto;
  static String vhost, usuario, clave, prefijo;
  static int ok = 0, fallidos = 0;

  public static void main(String[] args) {
    puerto = Integer.parseInt(args[0]);
    vhost = args[1]; usuario = args[2]; clave = args[3]; prefijo = args[4];
    CachingConnectionFactory consumidor = conexion();
    RabbitAdmin admin = new RabbitAdmin(consumidor);
    String exchange = prefijo + ".exchange";
    admin.declareExchange(new TopicExchange(exchange, true, false));

    caso("01-carrito-orders", "orden.creada",
        new com.pedidos360.carrito.messaging.OrdenCreadaEvent(UUID.fromString(ORDEN), "u-1",
            "comprador@example.cl",
            List.of(new com.pedidos360.carrito.messaging.OrdenCreadaEvent.Item(
                UUID.fromString(PRODUCTO), 2, 1990L)), 3980L, "CUPON10"),
        converter(com.pedidos360.carrito.config.RabbitMQConfig.class),
        converter(com.pedidos360.orders.config.RabbitMQConfig.class),
        com.pedidos360.orders.messaging.OrdenCreadaEvent.class,
        o -> {
          var e = (com.pedidos360.orders.messaging.OrdenCreadaEvent) o;
          igual(e.ordenId(), ORDEN, "ordenId");
          igual(e.usuarioId(), "u-1", "usuarioId");
          igual(e.emailComprador(), "comprador@example.cl", "emailComprador");
          igual(e.items().size(), 1, "items.size");
          igual(e.items().get(0).productoId(), PRODUCTO, "item.productoId");
          igual(e.items().get(0).cantidad(), 2, "item.cantidad");
          igual(e.totalClp(), 3980L, "totalClp");
          igual(e.codigoCupon(), "CUPON10", "codigoCupon");
        }, admin, exchange);

    caso("02-orders-productos", "orden.registrada",
        new com.pedidos360.orders.messaging.OrdenRegistradaEvent(UUID.fromString(ORDEN), "u-1",
            "comprador@example.cl",
            List.of(new com.pedidos360.orders.messaging.OrdenRegistradaEvent.Item(
                UUID.fromString(PRODUCTO), 2)), "CUPON10"),
        converter(com.pedidos360.orders.config.RabbitMQConfig.class),
        converter(com.pedidos360.productos.config.RabbitMQConfig.class),
        com.pedidos360.productos.messaging.OrdenRegistradaEvent.class,
        o -> verificarOrdenRegistrada(o), admin, exchange);

    caso("03-orders-notificaciones", "orden.registrada",
        new com.pedidos360.orders.messaging.OrdenRegistradaEvent(UUID.fromString(ORDEN), "u-1",
            "comprador@example.cl",
            List.of(new com.pedidos360.orders.messaging.OrdenRegistradaEvent.Item(
                UUID.fromString(PRODUCTO), 2)), "CUPON10"),
        converter(com.pedidos360.orders.config.RabbitMQConfig.class),
        converter(com.pedidos360.notificaciones.config.RabbitMQConfig.class),
        com.pedidos360.notificaciones.messaging.OrdenRegistradaEvent.class,
        o -> verificarOrdenRegistrada(o), admin, exchange);

    caso("04-orders-auditoria", "orden.registrada",
        new com.pedidos360.orders.messaging.OrdenRegistradaEvent(UUID.fromString(ORDEN), "u-1",
            "comprador@example.cl",
            List.of(new com.pedidos360.orders.messaging.OrdenRegistradaEvent.Item(
                UUID.fromString(PRODUCTO), 2)), "CUPON10"),
        converter(com.pedidos360.orders.config.RabbitMQConfig.class),
        converter(com.pedidos360.auditoria.config.RabbitMQConfig.class),
        Map.class,
        o -> {
          @SuppressWarnings("unchecked")
          Map<String, Object> m = (Map<String, Object>) o;
          igual(m.get("ordenId"), ORDEN, "ordenId");
          igual(m.get("usuarioId"), "u-1", "usuarioId");
          igual(m.get("emailComprador"), "comprador@example.cl", "emailComprador");
          igual(m.get("codigoCupon"), "CUPON10", "codigoCupon");
          igual(((List<?>) m.get("items")).size(), 1, "items.size");
          @SuppressWarnings("unchecked")
          Map<String, Object> item = (Map<String, Object>) ((List<?>) m.get("items")).get(0);
          igual(item.get("productoId"), PRODUCTO, "item.productoId");
          igual(item.get("cantidad"), 2, "item.cantidad");
        }, admin, exchange);

    caso("05-orders-cupones", "orden.registrada",
        new com.pedidos360.orders.messaging.OrdenRegistradaEvent(UUID.fromString(ORDEN), "u-1",
            "comprador@example.cl",
            List.of(new com.pedidos360.orders.messaging.OrdenRegistradaEvent.Item(
                UUID.fromString(PRODUCTO), 2)), "CUPON10"),
        converter(com.pedidos360.orders.config.RabbitMQConfig.class),
        converter(cl.pedidos360.cupones.config.RabbitMQConfig.class),
        cl.pedidos360.cupones.messaging.OrdenRegistradaEvent.class,
        o -> verificarOrdenRegistrada(o), admin, exchange);

    caso("06-productos-orders-reservado", "stock.reservado",
        new com.pedidos360.productos.messaging.StockReservadoEvent(
            UUID.fromString(EVENTO), UUID.fromString(ORDEN), "u-1", OCURRIO),
        converter(com.pedidos360.productos.config.RabbitMQConfig.class),
        converter(com.pedidos360.orders.config.RabbitMQConfig.class),
        com.pedidos360.orders.messaging.StockReservadoEvent.class,
        o -> {
          var e = (com.pedidos360.orders.messaging.StockReservadoEvent) o;
          igual(e.eventoId(), EVENTO, "eventoId");
          igual(e.ordenId(), ORDEN, "ordenId");
          igual(e.usuarioId(), "u-1", "usuarioId");
          igual(e.ocurrioEn(), OCURRIO, "ocurrioEn");
        }, admin, exchange);

    caso("07-productos-orders-rechazado", "stock.rechazado",
        new com.pedidos360.productos.messaging.StockRechazadoEvent(
            UUID.fromString(EVENTO), UUID.fromString(ORDEN), "u-1", "SIN_STOCK", OCURRIO),
        converter(com.pedidos360.productos.config.RabbitMQConfig.class),
        converter(com.pedidos360.orders.config.RabbitMQConfig.class),
        com.pedidos360.orders.messaging.StockRechazadoEvent.class,
        o -> {
          var e = (com.pedidos360.orders.messaging.StockRechazadoEvent) o;
          igual(e.eventoId(), EVENTO, "eventoId");
          igual(e.ordenId(), ORDEN, "ordenId");
          igual(e.usuarioId(), "u-1", "usuarioId");
          igual(e.motivo(), "SIN_STOCK", "motivo");
          igual(e.ocurrioEn(), OCURRIO, "ocurrioEn");
        }, admin, exchange);

    caso("08-productos-orders-liberado", "stock.liberado",
        new com.pedidos360.productos.messaging.StockLiberadoEvent(
            UUID.fromString(EVENTO), UUID.fromString(ORDEN), "u-1", "EXPIRADA", OCURRIO),
        converter(com.pedidos360.productos.config.RabbitMQConfig.class),
        converter(com.pedidos360.orders.config.RabbitMQConfig.class),
        com.pedidos360.orders.messaging.StockLiberadoEvent.class,
        o -> {
          var e = (com.pedidos360.orders.messaging.StockLiberadoEvent) o;
          igual(e.eventoId(), EVENTO, "eventoId");
          igual(e.ordenId(), ORDEN, "ordenId");
          igual(e.usuarioId(), "u-1", "usuarioId");
          igual(e.motivo(), "EXPIRADA", "motivo");
          igual(e.ocurrioEn(), OCURRIO, "ocurrioEn");
        }, admin, exchange);

    System.out.println("RESUMEN_SCRIPT casos=8 correctos=" + ok + " fallidos=" + fallidos);
    consumidor.destroy();
    if (fallidos > 0) System.exit(1);
  }

  /** Los beans jsonMessageConverter() son package-private: se invocan por
   *  reflexion desde su propia clase, sin meter el helper en esos paquetes. */
  static MessageConverter converter(Class<?> config) {
    try {
      Method metodo = config.getDeclaredMethod("jsonMessageConverter");
      metodo.setAccessible(true);
      return (MessageConverter) metodo.invoke(config.getDeclaredConstructor().newInstance());
    } catch (Exception e) {
      throw new IllegalStateException("No se pudo obtener el converter real de " + config.getName(), e);
    }
  }

  static CachingConnectionFactory conexion() {
    CachingConnectionFactory cf = new CachingConnectionFactory("127.0.0.1", puerto);
    cf.setVirtualHost(vhost);
    cf.setUsername(usuario);
    cf.setPassword(clave);
    return cf;
  }

  static void verificarOrdenRegistrada(Object o) {
    // Los tres consumidores tienen el mismo espejo pero en paquetes distintos.
    if (o instanceof com.pedidos360.productos.messaging.OrdenRegistradaEvent e) {
      igual(e.ordenId(), ORDEN, "ordenId");
      igual(e.usuarioId(), "u-1", "usuarioId");
      igual(e.emailComprador(), "comprador@example.cl", "emailComprador");
      igual(e.items().size(), 1, "items.size");
      igual(e.items().get(0).productoId(), PRODUCTO, "item.productoId");
      igual(e.items().get(0).cantidad(), 2, "item.cantidad");
      igual(e.codigoCupon(), "CUPON10", "codigoCupon");
    } else if (o instanceof com.pedidos360.notificaciones.messaging.OrdenRegistradaEvent e) {
      igual(e.ordenId(), ORDEN, "ordenId");
      igual(e.usuarioId(), "u-1", "usuarioId");
      igual(e.emailComprador(), "comprador@example.cl", "emailComprador");
      igual(e.items().size(), 1, "items.size");
      igual(e.items().get(0).productoId(), PRODUCTO, "item.productoId");
      igual(e.items().get(0).cantidad(), 2, "item.cantidad");
      igual(e.codigoCupon(), "CUPON10", "codigoCupon");
    } else if (o instanceof cl.pedidos360.cupones.messaging.OrdenRegistradaEvent e) {
      igual(e.ordenId(), ORDEN, "ordenId");
      igual(e.usuarioId(), "u-1", "usuarioId");
      igual(e.emailComprador(), "comprador@example.cl", "emailComprador");
      igual(e.items().size(), 1, "items.size");
      igual(e.items().get(0).productoId(), PRODUCTO, "item.productoId");
      igual(e.items().get(0).cantidad(), 2, "item.cantidad");
      igual(e.codigoCupon(), "CUPON10", "codigoCupon");
    } else {
      throw new AssertionError("Espejo inesperado: " + o.getClass().getName());
    }
  }

  static void caso(String nombre, String routingKey, Object evento,
      MessageConverter converterProductor, MessageConverter converterConsumidor,
      Class<?> claseEsperada, Consumer<Object> verificacion, RabbitAdmin admin, String exchange) {
    String queue = prefijo + ".q." + nombre;
    org.springframework.amqp.core.Queue cola = QueueBuilder.nonDurable(queue).autoDelete().build();
    admin.declareQueue(cola);
    admin.declareBinding(BindingBuilder.bind(cola)
        .to(new TopicExchange(exchange, true, false)).with(routingKey));

    CachingConnectionFactory cfListener = conexion();
    CachingConnectionFactory cfProductorCase = conexion();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Object> recibido = new AtomicReference<>();
    AtomicReference<String> typeId = new AtomicReference<>();
    AtomicReference<Throwable> error = new AtomicReference<>();

    SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(cfListener);
    container.setQueueNames(queue);
    container.setMessageListener(message -> {
      try {
        recibido.set(converterConsumidor.fromMessage(message));
        typeId.set((String) message.getMessageProperties().getHeader(TYPE_ID));
      } catch (Throwable t) {
        error.set(t);
      } finally {
        latch.countDown();
      }
    });
    container.afterPropertiesSet();
    container.start();
    try {
      RabbitTemplate template = new RabbitTemplate(cfProductorCase);
      template.setMessageConverter(converterProductor);
      template.convertAndSend(exchange, routingKey, evento);
      if (!latch.await(20, TimeUnit.SECONDS)) {
        throw new AssertionError("el listener no recibio el mensaje en 20 s");
      }
      if (error.get() != null) {
        throw new AssertionError("conversion fallo en el listener", error.get());
      }
      Object o = recibido.get();
      if (o == null || !claseEsperada.isInstance(o)) {
        throw new AssertionError("clase resolvida " + (o == null ? "null" : o.getClass().getName())
            + " != " + claseEsperada.getName());
      }
      verificacion.accept(o);
      System.out.println("CASO " + nombre + " OK __TypeId__=" + typeId.get()
          + " listener=" + o.getClass().getName());
      ok++;
    } catch (Throwable t) {
      fallidos++;
      System.out.println("CASO " + nombre + " FALLO " + t);
      if (t.getCause() != null) System.out.println("  causa=" + t.getCause());
    } finally {
      container.stop();
      cfListener.destroy();
      cfProductorCase.destroy();
      admin.deleteQueue(queue);
    }
  }

  static void igual(Object actual, Object esperado, String campo) {
    if (!Objects.equals(String.valueOf(actual), String.valueOf(esperado))) {
      throw new AssertionError(campo + ": esperado=" + esperado + " actual=" + actual);
    }
  }
}
'@
  $fuente = Join-Path $tmp 'ProbarDtoRabbit.java'
  [System.IO.File]::WriteAllText($fuente, $codigoFuente, [System.Text.UTF8Encoding]::new($false))

  # Levantar RabbitMQ temporal (usuario propio: guest esta restringido a localhost).
  $id = & docker run -d --name $contenedor -p ('127.0.0.1:{0}:5672' -f $puerto) `
    -e "RABBITMQ_DEFAULT_USER=$usuario" -e "RABBITMQ_DEFAULT_PASS=$clave" `
    -e "RABBITMQ_DEFAULT_VHOST=$vhost" `
    rabbitmq:3.13-management-alpine 2>&1
  if ($LASTEXITCODE -ne 0) { throw "No se pudo levantar RabbitMQ: $id" }
  Write-Host "CONTENEDOR: $id"

  # Esperar a que el broker acepte clientesAMQP: el proxy TCP abre antes que
  # el listener AMQP. No ejecutar rabbitmq-diagnostics durante el arranque.
  $listo = $false
  for ($i = 0; $i -lt 90; $i++) {
    $lista = & docker logs $contenedor 2>&1 | Select-String 'Ready to start client connection listeners'
    if ($lista) { $listo = $true; break }
    Start-Sleep -Seconds 1
  }
  if (-not $listo) { throw "RabbitMQ no quedo listo para clientes en 90 s" }
  $ping = & docker exec $contenedor rabbitmq-diagnostics -q ping 2>&1
  if ($LASTEXITCODE -ne 0) { throw "RabbitMQ no respondio ping: $ping" }
  Write-Host "BROKER listo tras $i s (ping: $ping)"

  & javac -encoding UTF-8 -cp $cp -d $tmp $fuente
  if ($LASTEXITCODE -ne 0) { throw "javac fallo" }
  Write-Host "HELPER compilado con converter real de los 6 repos"

  & java -cp ($tmp + ';' + $cp) ProbarDtoRabbit "$puerto" "$vhost" "$usuario" "$clave" "$sufijo"
  $codigo = $LASTEXITCODE
}
catch {
  Write-Host "ERROR: $($_.Exception.Message)"
  Write-Host "DETALLE: $($_.InvocationInfo.PositionMessage)"
  if ($contenedor) { Write-Host "=== LOGS BROKER ==="; & docker logs --tail 60 $contenedor 2>&1 }
  $codigo = 1
}
finally {
  if ($contenedor) {
    if ($env:P360_KEEP -eq '1') {
      Write-Host "CONTENEDOR $contenedor conservado (P360_KEEP=1)"
    } else {
      & docker rm -f $contenedor 2>&1 | Out-Null
      Write-Host "CONTENEDOR $contenedor detenido y borrado (finally)"
    }
  }
  if ($tmp -and (Test-Path $tmp)) { Remove-Item $tmp -Recurse -Force -ErrorAction SilentlyContinue }
}

Write-Host "RESULTADO script exit=$codigo"
exit $codigo
