package com.pedidos360.admin.kafka;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.test.web.servlet.MockMvc;

@EnabledIfEnvironmentVariable(named = "P360_TEST_KAFKA", matches = "true")
@SpringBootTest
@AutoConfigureMockMvc
class KafkaAdminEnVivoTest {
  private static final String API = "/api/kafka";
  @Autowired MockMvc mvc;
  @Value("${p360.kafka.bootstrap-servers}") String bootstrap;

  @Test
  void administraTopicYGrupoDesechablesConBrokerReal() throws Exception {
    var admin = jwt().authorities(new SimpleGrantedAuthority("ROLE_Admin"));
    var nombre = "p360.demo." + UUID.randomUUID();
    String topic = API + "/topics/" + nombre;
    String group = API + "/groups/" + nombre;
    mvc.perform(get(API + "/topics")).andExpect(status().isUnauthorized());
    mvc.perform(get(API + "/topics").with(jwt())).andExpect(status().isForbidden());
    mvc.perform(post(API + "/topics").with(admin).contentType("application/json")
        .content("""
            {"nombre":"%s","particiones":3,"replicas":3,"retencionMs":60000,"limpieza":"delete"}
            """.formatted(nombre))).andExpect(status().isCreated());
    try {
      mvc.perform(get(topic).with(admin)).andExpect(status().isOk())
          .andExpect(jsonPath("$.particiones.length()").value(3))
          .andExpect(jsonPath("$.particiones[0].replicas.length()").value(3));
      mvc.perform(patch(topic).with(admin).contentType("application/json")
          .content("{\"retencionMs\":120000}")).andExpect(status().isNoContent());
      mvc.perform(get(topic).with(admin)).andExpect(jsonPath("$.configuracion['retention.ms']").value("120000"));

      var p0 = new TopicPartition(nombre, 0);
      try (var consumer = new KafkaConsumer<>(Map.of("bootstrap.servers", bootstrap, "group.id", nombre),
          new StringDeserializer(), new StringDeserializer())) {
        consumer.assign(List.of(p0));
        consumer.commitSync(Map.of(p0, new OffsetAndMetadata(0)));
      }
      try {
        mvc.perform(get(group).with(admin)).andExpect(status().isOk())
            .andExpect(jsonPath("$.posiciones[0].confirmado").value(0))
            .andExpect(jsonPath("$.posiciones[0].lag").value(0));
      } finally {
        mvc.perform(delete(group).with(admin)).andExpect(status().isNoContent());
      }
    } finally {
      mvc.perform(delete(topic).with(admin)).andExpect(status().isNoContent());
    }
  }
}
