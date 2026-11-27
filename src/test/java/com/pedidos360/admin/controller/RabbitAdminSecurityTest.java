package com.pedidos360.admin.controller;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.options;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.pedidos360.admin.service.RabbitAdminService;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.AmqpException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.web.servlet.MockMvc;

// Identidades 100% simuladas: las "tokens" son cadenas de prueba, nunca JWT reales.
// Filtros de seguridad REALES (@AutoConfigureMockMvc sin addFilters=false); solo se
// sustituye el decoder externo y el servicio/broker para no depender de la red.
@SpringBootTest
@AutoConfigureMockMvc
class RabbitAdminSecurityTest {
  private static final String ADMIN = "entra-admin-de-prueba";
  private static final String CLIENTE = "entra-cliente-de-prueba";
  private static final String INVALIDO = "jwt-invalido-de-prueba";

  @Autowired MockMvc mvc;
  @MockBean JwtDecoder decoder;
  @MockBean Admin kafka;
  @MockBean RabbitAdminService admin;

  private void conRol(String token, String rol) {
    when(decoder.decode(token)).thenReturn(Jwt.withTokenValue(token)
        .header("alg", "RS256").subject("identidad-de-prueba")
        .claim("roles", List.of(rol)).build());
  }

  private static org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder cola() {
    return post("/api/rabbitmq/queues").contentType(MediaType.APPLICATION_JSON)
        .content("{\"nombre\":\"pedidos\"}");
  }

  @Test
  void crearCola_sinJwt_devuelve401() throws Exception {
    // Origin permitido: CORS no puede abrir la escritura, el POST real sigue exigiendo JWT.
    mvc.perform(cola().header("Origin", "http://localhost:4200")
        .header("Content-Type", "application/json"))
        .andExpect(status().isUnauthorized());
    verifyNoInteractions(admin);
  }

  @Test
  void crearCola_clienteEntra_devuelve403() throws Exception {
    conRol(CLIENTE, "Cliente");
    mvc.perform(cola().header("Authorization", "Bearer " + CLIENTE))
        .andExpect(status().isForbidden());
    verifyNoInteractions(admin);
  }

  @Test
  void crearCola_adminEntra_devuelve201DelController() throws Exception {
    conRol(ADMIN, "Admin");
    when(admin.crearCola("pedidos", null)).thenReturn("pedidos");

    mvc.perform(cola().header("Authorization", "Bearer " + ADMIN))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.nombre").value("pedidos"));
    verify(admin).crearCola("pedidos", null);
  }

  @Test
  void crearCola_jwtInvalido_devuelve401() throws Exception {
    when(decoder.decode(INVALIDO)).thenThrow(new BadJwtException("Firma inválida de prueba"));
    mvc.perform(cola().header("Authorization", "Bearer " + INVALIDO))
        .andExpect(status().isUnauthorized());
    verifyNoInteractions(admin);
  }

  @Test
  void eliminarBinding_sinJwt_devuelve401() throws Exception {
    mvc.perform(delete("/api/rabbitmq/bindings").contentType(MediaType.APPLICATION_JSON)
        .content("{\"cola\":\"c\",\"exchange\":\"e\",\"routingKey\":\"k\"}")
        .header("Origin", "http://localhost:4200"))
        .andExpect(status().isUnauthorized());
    verifyNoInteractions(admin);
  }

  @Test
  void eliminarBinding_clienteEntra_devuelve403() throws Exception {
    conRol(CLIENTE, "Cliente");
    mvc.perform(delete("/api/rabbitmq/bindings").contentType(MediaType.APPLICATION_JSON)
        .content("{\"cola\":\"c\",\"exchange\":\"e\"}")
        .header("Authorization", "Bearer " + CLIENTE))
        .andExpect(status().isForbidden());
    verifyNoInteractions(admin);
  }

  @Test
  void eliminarBinding_adminEntra_devuelve204DelController() throws Exception {
    conRol(ADMIN, "Admin");
    mvc.perform(delete("/api/rabbitmq/bindings").contentType(MediaType.APPLICATION_JSON)
        .content("{\"cola\":\"c\",\"exchange\":\"e\",\"routingKey\":\"k\"}")
        .header("Authorization", "Bearer " + ADMIN))
        .andExpect(status().isNoContent());
    verify(admin).eliminarBinding("c", "e", "k");
  }

  @Test
  void purgarCola_sinJwt_devuelve401() throws Exception {
    mvc.perform(post("/api/rabbitmq/queues/c/purge")).andExpect(status().isUnauthorized());
    verifyNoInteractions(admin);
  }

  @Test
  void purgarCola_clienteEntra_devuelve403() throws Exception {
    conRol(CLIENTE, "Cliente");
    mvc.perform(post("/api/rabbitmq/queues/c/purge")
        .header("Authorization", "Bearer " + CLIENTE)).andExpect(status().isForbidden());
    verifyNoInteractions(admin);
  }

  @Test
  void purgarCola_adminEntra_devuelve200DelController() throws Exception {
    conRol(ADMIN, "Admin");
    when(admin.purgarCola("c")).thenReturn(3);
    mvc.perform(post("/api/rabbitmq/queues/c/purge")
        .header("Authorization", "Bearer " + ADMIN))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.eliminados").value(3));
    verify(admin).purgarCola("c");
  }

  @Test
  void preflightDelOrigenPermitido_devuelve200SinLlamarServicios() throws Exception {
    // El navegador NO envía JWT en el preflight: debe responder CORS, no 401.
    mvc.perform(options("/api/rabbitmq/queues")
        .header("Origin", "http://localhost:4200")
        .header("Access-Control-Request-Method", "POST")
        .header("Access-Control-Request-Headers", "Authorization,Content-Type"))
        .andExpect(status().is2xxSuccessful())
        .andExpect(header().string("Access-Control-Allow-Origin", "http://localhost:4200"));
    verifyNoInteractions(admin);
  }

  @Test
  void preflightDeOtroOrigen_devuelve403SinAllowOrigin() throws Exception {
    mvc.perform(options("/api/rabbitmq/queues")
        .header("Origin", "https://origen-no-permitido.invalid")
        .header("Access-Control-Request-Method", "POST")
        .header("Access-Control-Request-Headers", "Authorization,Content-Type"))
        .andExpect(status().isForbidden())
        .andExpect(header().doesNotExist("Access-Control-Allow-Origin"));
    verifyNoInteractions(admin);
  }

  @Test
  void crearCola_adminConNombreVacio_devuelve400() throws Exception {
    conRol(ADMIN, "Admin");
    mvc.perform(post("/api/rabbitmq/queues").contentType(MediaType.APPLICATION_JSON)
        .content("{\"nombre\":\"  \"}")
        .header("Authorization", "Bearer " + ADMIN))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.errores.nombre").exists());
    verifyNoInteractions(admin);
  }

  @Test
  void infoCola_adminInexistente_devuelve404() throws Exception {
    conRol(ADMIN, "Admin");
    when(admin.infoCola("nadie")).thenThrow(new NoSuchElementException("No existe la cola 'nadie'."));
    mvc.perform(get("/api/rabbitmq/queues/nadie")
        .header("Authorization", "Bearer " + ADMIN)).andExpect(status().isNotFound());
  }

  @Test
  void crearCola_adminConBrokerCaido_devuelve503() throws Exception {
    conRol(ADMIN, "Admin");
    when(admin.crearCola("pedidos", null)).thenThrow(new AmqpException("broker caído de prueba"));
    mvc.perform(cola().header("Authorization", "Bearer " + ADMIN))
        .andExpect(status().isServiceUnavailable());
  }
}
