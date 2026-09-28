package com.pedidos360.admin.controller;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.pedidos360.admin.service.RabbitAdminService;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(RabbitAdminController.class)
class RabbitAdminControllerTest {

  @Autowired MockMvc mvc;
  @MockBean RabbitAdminService admin;

  @Test
  void crearCola_nombreVacio_devuelve400() throws Exception {
    mvc.perform(
            post("/api/rabbitmq/queues")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"nombre\":\"  \"}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.errores.nombre").exists());
  }

  @Test
  void crearCola_valida_devuelve201() throws Exception {
    when(admin.crearCola(eq("pedidos"), isNull())).thenReturn("pedidos");

    mvc.perform(
            post("/api/rabbitmq/queues")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"nombre\":\"pedidos\"}"))
        .andExpect(status().isCreated())
        .andExpect(jsonPath("$.nombre").value("pedidos"));
  }

  @Test
  void infoCola_inexistente_devuelve404() throws Exception {
    when(admin.infoCola("nadie"))
        .thenThrow(new NoSuchElementException("No existe la cola 'nadie'."));

    mvc.perform(get("/api/rabbitmq/queues/nadie")).andExpect(status().isNotFound());
  }
}
