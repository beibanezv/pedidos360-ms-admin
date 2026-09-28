package com.pedidos360.admin.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

@ExtendWith(MockitoExtension.class)
class RabbitAdminServiceTest {

  @Mock RabbitAdmin rabbitAdmin;
  @InjectMocks RabbitAdminService service;

  @Test
  void crearCola_sinDurable_declaraColaDurable() {
    service.crearCola("pedidos", null);

    var captor = ArgumentCaptor.forClass(Queue.class);
    verify(rabbitAdmin).declareQueue(captor.capture());
    assertEquals("pedidos", captor.getValue().getName());
    assertTrue(captor.getValue().isDurable());
  }

  @Test
  void crearExchange_tipoInvalido_noTocaElBroker() {
    assertThrows(IllegalArgumentException.class, () -> service.crearExchange("x", "headers"));
    verifyNoInteractions(rabbitAdmin);
  }

  @Test
  void infoCola_inexistente_lanzaExcepcion() {
    when(rabbitAdmin.getQueueInfo("nadie")).thenReturn(null);

    assertThrows(NoSuchElementException.class, () -> service.infoCola("nadie"));
  }

  @Test
  void purgarCola_existente_devuelveCantidad() {
    when(rabbitAdmin.getQueueInfo("c")).thenReturn(mock(QueueInformation.class));
    when(rabbitAdmin.purgeQueue("c")).thenReturn(3);

    assertEquals(3, service.purgarCola("c"));
  }
}
