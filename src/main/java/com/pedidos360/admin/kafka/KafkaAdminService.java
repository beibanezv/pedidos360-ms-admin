package com.pedidos360.admin.kafka;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
class KafkaAdminService {
  private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);
  private final Admin admin;

  KafkaAdminService(Admin admin) {
    this.admin = admin;
  }

  List<String> topics() {
    return esperar(admin.listTopics().names()).stream().sorted().toList();
  }

  Tema tema(String nombre) {
    var descripcion = esperar(admin.describeTopics(List.of(nombre)).topicNameValues().get(nombre));
    var recurso = recurso(nombre);
    var config = esperar(admin.describeConfigs(List.of(recurso)).values().get(recurso));
    return new Tema(nombre, descripcion.partitions().stream()
        .map(p -> new Particion(p.partition(), p.leader() == null ? -1 : p.leader().id(),
            p.replicas().stream().map(Node::id).toList(), p.isr().stream().map(Node::id).toList()))
        .toList(), Map.of("retention.ms", config.get("retention.ms").value(),
        "cleanup.policy", config.get("cleanup.policy").value(),
        "min.insync.replicas", config.get("min.insync.replicas").value()));
  }

  void crear(String nombre, int particiones, short replicas, long retencionMs, String limpieza) {
    exigirDesechable(nombre);
    var topic = new NewTopic(nombre, particiones, replicas).configs(Map.of(
        "retention.ms", Long.toString(retencionMs), "cleanup.policy", limpieza,
        "min.insync.replicas", replicas >= 2 ? "2" : "1"));
    esperar(admin.createTopics(List.of(topic)).all());
    log.info("Topic creado: {}", nombre);
  }

  void actualizar(String nombre, Integer particiones, Long retencionMs, String limpieza) {
    exigirDesechable(nombre);
    if (particiones == null && retencionMs == null && limpieza == null) {
      throw new IllegalArgumentException("Indica al menos un cambio.");
    }
    if (particiones != null) {
      int actuales = esperar(admin.describeTopics(List.of(nombre)).topicNameValues().get(nombre))
          .partitions().size();
      if (particiones < actuales) throw new IllegalArgumentException("Kafka no permite reducir particiones.");
      if (particiones > actuales) {
        esperar(admin.createPartitions(Map.of(nombre, NewPartitions.increaseTo(particiones))).all());
      }
    }
    var cambios = new ArrayList<AlterConfigOp>();
    if (retencionMs != null) cambios.add(set("retention.ms", retencionMs.toString()));
    if (limpieza != null) cambios.add(set("cleanup.policy", limpieza));
    if (!cambios.isEmpty()) esperar(admin.incrementalAlterConfigs(Map.of(recurso(nombre), cambios)).all());
    log.info("Topic actualizado: {}", nombre);
  }

  void eliminar(String nombre) {
    exigirDesechable(nombre);
    esperar(admin.deleteTopics(List.of(nombre)).all());
    log.info("Topic eliminado: {}", nombre);
  }

  List<String> grupos() {
    return esperar(admin.listConsumerGroups().all()).stream().map(g -> g.groupId()).sorted().toList();
  }

  Grupo grupo(String nombre) {
    var grupo = esperar(admin.describeConsumerGroups(List.of(nombre)).describedGroups().get(nombre));
    var posiciones = esperar(admin.listConsumerGroupOffsets(nombre).partitionsToOffsetAndMetadata());
    var ultima = posiciones.isEmpty() ? Map.<org.apache.kafka.common.TopicPartition,
        org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo>of()
        : esperar(admin.listOffsets(posiciones.keySet().stream()
            .collect(Collectors.toMap(p -> p, p -> OffsetSpec.latest()))).all());
    return new Grupo(nombre, grupo.state().toString(), grupo.members().size(),
        posiciones.entrySet().stream()
            .map(e -> new Posicion(e.getKey().topic(), e.getKey().partition(), e.getValue().offset(),
                ultima.get(e.getKey()).offset()))
            .sorted(Comparator.comparing(Posicion::topic).thenComparingInt(Posicion::particion)).toList());
  }

  void eliminarGrupo(String nombre) {
    exigirDesechable(nombre);
    esperar(admin.deleteConsumerGroups(List.of(nombre)).all());
    log.info("Grupo eliminado: {}", nombre);
  }

  private static void exigirDesechable(String nombre) {
    if (!nombre.matches("p360\\.demo\\.[a-zA-Z0-9._-]{1,80}")) {
      throw new IllegalArgumentException("Nombre protegido: administra recursos desechables con prefijo p360.demo.");
    }
  }

  private static ConfigResource recurso(String nombre) {
    return new ConfigResource(ConfigResource.Type.TOPIC, nombre);
  }

  private static AlterConfigOp set(String clave, String valor) {
    return new AlterConfigOp(new ConfigEntry(clave, valor), AlterConfigOp.OpType.SET);
  }

  private static <T> T esperar(KafkaFuture<T> futuro) {
    try {
      return futuro.get(5, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Operación Kafka interrumpida");
    } catch (TimeoutException | ExecutionException ex) {
      var causa = ex.getCause();
      if (causa instanceof UnknownTopicOrPartitionException || causa instanceof GroupIdNotFoundException) {
        throw new NoSuchElementException("Recurso Kafka no encontrado");
      }
      if (causa instanceof TopicExistsException || causa instanceof GroupNotEmptyException) {
        throw new ResponseStatusException(HttpStatus.CONFLICT, "El recurso ya existe o sigue activo");
      }
      if (causa instanceof InvalidConfigurationException || causa instanceof InvalidPartitionsException) {
        throw new IllegalArgumentException("Kafka rechazó la configuración");
      }
      log.warn("Operación Kafka no completada: {}", causa == null ? ex.getClass() : causa.getClass());
      throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Kafka no completó la operación");
    }
  }

  record Tema(String nombre, List<Particion> particiones, Map<String, String> configuracion) {}
  record Particion(int particion, int lider, List<Integer> replicas, List<Integer> isr) {}
  record Grupo(String nombre, String estado, int miembros, List<Posicion> posiciones) {}
  record Posicion(String topic, int particion, long confirmado, long finalLog) {
    @com.fasterxml.jackson.annotation.JsonProperty public long lag() {
      return Math.max(0, finalLog - confirmado);
    }
  }
}
