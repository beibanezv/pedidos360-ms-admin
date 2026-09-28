package com.pedidos360.admin.config;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// Rúbrica 3: la configuración de mensajería vive acá, separada de la lógica.
// El servicio solo recibe el RabbitAdmin ya armado.
@Configuration
public class RabbitAdminConfig {

  // Gestiona colas/exchanges/bindings en tiempo de ejecución.
  // La conexión sale de spring.rabbitmq.* (env vars, ver application.properties).
  @Bean
  public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
    return new RabbitAdmin(connectionFactory);
  }
}
