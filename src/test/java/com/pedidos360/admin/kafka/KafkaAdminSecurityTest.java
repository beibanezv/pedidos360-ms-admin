package com.pedidos360.admin.kafka;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.pedidos360.admin.service.RabbitAdminService;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.web.servlet.MockMvc;

// Identidades 100% simuladas: tokens de prueba, sin JWT reales ni red externa.
@SpringBootTest
@AutoConfigureMockMvc
class KafkaAdminSecurityTest {
  private static final String COGNITO = "cognito-de-prueba";
  private static final String ADMIN = "entra-admin-de-prueba";
  private static final String CLIENTE = "entra-cliente";

  @Autowired MockMvc mvc;
  @MockBean JwtDecoder decoder;
  @MockBean Admin kafka;
  @MockBean RabbitAdminService rabbit;

  @Test
  void bearerDeCognitoNoEsAdmitidoEnRabbit() throws Exception {
    when(decoder.decode(COGNITO)).thenThrow(new BadJwtException("Issuer no Entra"));
    mvc.perform(get("/api/rabbitmq/queues/no-existe")
        .header("Authorization", "Bearer " + COGNITO))
        .andExpect(status().isUnauthorized());
    verifyNoInteractions(rabbit);
  }

  @Test
  void soloEntraAdminEsAdmitidoEnKafka() throws Exception {
    when(decoder.decode(COGNITO)).thenThrow(new BadJwtException("Issuer no Entra"));
    when(decoder.decode(CLIENTE)).thenReturn(Jwt.withTokenValue(CLIENTE)
        .header("alg", "RS256").subject("cliente").claim("roles", List.of("Cliente")).build());
    mvc.perform(get("/api/kafka/topics")).andExpect(status().isUnauthorized());
    mvc.perform(get("/api/kafka/topics").header("Authorization", "Bearer " + COGNITO))
        .andExpect(status().isUnauthorized());
    mvc.perform(get("/api/kafka/topics").header("Authorization", "Bearer " + CLIENTE))
        .andExpect(status().isForbidden());
  }

  @Test
  void adminEntraObtieneRespuestaRealDeKafka() throws Exception {
    when(decoder.decode(ADMIN)).thenReturn(Jwt.withTokenValue(ADMIN)
        .header("alg", "RS256").subject("admin").claim("roles", List.of("Admin")).build());
    var resultados = mock(ListTopicsResult.class);
    when(kafka.listTopics()).thenReturn(resultados);
    when(resultados.names()).thenReturn(KafkaFuture.completedFuture(Set.of("p360.demo.t")));

    mvc.perform(get("/api/kafka/topics").header("Authorization", "Bearer " + ADMIN))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0]").value("p360.demo.t"));
  }

  @Test
  void preflightKafkaSinJwt_devuelve401() throws Exception {
    mvc.perform(options("/api/kafka/topics")
        .header("Origin", "https://origen-de-prueba.invalid")
        .header("Access-Control-Request-Method", "GET"))
        .andExpect(status().isUnauthorized());
    verifyNoInteractions(rabbit);
  }
}
