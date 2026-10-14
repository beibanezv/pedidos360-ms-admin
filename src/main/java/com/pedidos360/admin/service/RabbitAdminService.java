package com.pedidos360.admin.service;

import java.util.NoSuchElementException;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

// Rúbrica 7: toda la lógica RabbitMQ vive acá. El controlador llama a estos
// métodos de alto nivel y no conoce detalles del broker.
@Service
public class RabbitAdminService {

  private final RabbitAdmin rabbitAdmin;

  public RabbitAdminService(RabbitAdmin rabbitAdmin) {
    this.rabbitAdmin = rabbitAdmin;
  }

  // Crea una cola durable por defecto (sobrevive reinicios).
  // Idempotente: si ya existe con los mismos atributos, no falla.
  public String crearCola(String nombre, Boolean durable) {
    boolean duradera = durable == null || durable;
    Queue cola =
        duradera ? QueueBuilder.durable(nombre).build() : QueueBuilder.nonDurable(nombre).build();
    rabbitAdmin.declareQueue(cola);
    return nombre;
  }

  // Crea un exchange durable. Tipos válidos: direct, topic, fanout.
  public String crearExchange(String nombre, String tipo) {
    rabbitAdmin.declareExchange(exchangeSegunTipo(nombre, tipo));
    return nombre;
  }

  private Exchange exchangeSegunTipo(String nombre, String tipo) {
    return switch (tipo.toLowerCase()) {
      case "direct" -> new DirectExchange(nombre, true, false);
      case "topic" -> new TopicExchange(nombre, true, false);
      case "fanout" -> new FanoutExchange(nombre, true, false);
      default ->
          throw new IllegalArgumentException(
              "Tipo de exchange inválido: '" + tipo + "'. Válidos: direct, topic, fanout.");
    };
  }

  // Enlaza una cola existente a un exchange existente con un Binding directo
  // (no BindingBuilder con colas/exchanges falsos, que solo sirve para direct
  // y no apunta a lo ya declarado). Vale para direct, topic y fanout;
  // en fanout la routing key se ignora y puede omitirse.
  public void crearBinding(String cola, String exchange, String routingKey) {
    String clave = routingKey == null ? "" : routingKey;
    rabbitAdmin.declareBinding(
        new Binding(cola, Binding.DestinationType.QUEUE, exchange, clave, null));
  }

  // La instrucción de EP3 pide crear Y eliminar colas, exchanges y bindings.
  // Misma normalización que crearBinding (null -> "") para que el Binding
  // calce con el declarado; removeBinding es idempotente (no falla si no existe).
  public void eliminarBinding(String cola, String exchange, String routingKey) {
    String clave = routingKey == null ? "" : routingKey;
    rabbitAdmin.removeBinding(
        new Binding(cola, Binding.DestinationType.QUEUE, exchange, clave, null));
  }

  public QueueInfo infoCola(String nombre) {
    var info = rabbitAdmin.getQueueInfo(nombre);
    if (info == null) {
      throw new NoSuchElementException("No existe la cola '" + nombre + "'.");
    }
    return new QueueInfo(info.getName(), info.getMessageCount(), info.getConsumerCount());
  }

  public void eliminarCola(String nombre) {
    exigirCola(nombre);
    rabbitAdmin.deleteQueue(nombre);
  }

  public void eliminarExchange(String nombre) {
    if (!rabbitAdmin.deleteExchange(nombre)) {
      throw new NoSuchElementException("No existe el exchange '" + nombre + "'.");
    }
  }

  // Vacía la cola y devuelve cuántos mensajes eliminó.
  public int purgarCola(String nombre) {
    exigirCola(nombre);
    return rabbitAdmin.purgeQueue(nombre);
  }

  private void exigirCola(String nombre) {
    if (rabbitAdmin.getQueueInfo(nombre) == null) {
      throw new NoSuchElementException("No existe la cola '" + nombre + "'.");
    }
  }
}
