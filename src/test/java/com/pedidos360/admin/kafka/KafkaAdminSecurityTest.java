package com.pedidos360.admin.kafka;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.pedidos360.admin.service.RabbitAdminService;
import java.util.List;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
class KafkaAdminSecurityTest {
  @Autowired MockMvc mvc;
  @MockBean JwtDecoder decoder;
  @MockBean Admin kafka;
  @MockBean RabbitAdminService rabbit;

  @Test
  void bearerDeCognitoNoSeProcesaEnRutaRabbitPreexistente() throws Exception {
    when(decoder.decode("cognito-de-prueba")).thenThrow(new BadJwtException("Issuer no Entra"));
    when(rabbit.infoCola("no-existe")).thenThrow(new java.util.NoSuchElementException("no existe"));
    mvc.perform(get("/api/rabbitmq/queues/no-existe").header("Authorization", "Bearer cognito-de-prueba"))
        .andExpect(status().isNotFound());
  }

  @Test
  void soloEntraAdminEsAdmitidoEnKafka() throws Exception {
    when(decoder.decode("cognito-de-prueba")).thenThrow(new BadJwtException("Issuer no Entra"));
    when(decoder.decode("entra-cliente")).thenReturn(Jwt.withTokenValue("entra-cliente")
        .header("alg", "RS256").subject("cliente").claim("roles", List.of("Cliente")).build());
    mvc.perform(get("/api/kafka/topics")).andExpect(status().isUnauthorized());
    mvc.perform(get("/api/kafka/topics").header("Authorization", "Bearer cognito-de-prueba"))
        .andExpect(status().isUnauthorized());
    mvc.perform(get("/api/kafka/topics").header("Authorization", "Bearer entra-cliente"))
        .andExpect(status().isForbidden());
  }
}
