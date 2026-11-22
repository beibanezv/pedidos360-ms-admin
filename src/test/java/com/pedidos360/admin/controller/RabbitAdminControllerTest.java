package com.pedidos360.admin.controller;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.pedidos360.admin.service.RabbitAdminService;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(RabbitAdminController.class)
@AutoConfigureMockMvc(addFilters = false)
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

  @Test
  void eliminarBinding_valido_devuelve204() throws Exception {
    mvc.perform(
            delete("/api/rabbitmq/bindings")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"cola\":\"c\",\"exchange\":\"e\",\"routingKey\":\"k\"}"))
        .andExpect(status().isNoContent());

    verify(admin).eliminarBinding("c", "e", "k");
  }

  @Test
  void eliminarBinding_sinCola_devuelve400() throws Exception {
    mvc.perform(
            delete("/api/rabbitmq/bindings")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"exchange\":\"e\"}"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.errores.cola").exists());
  }
}
