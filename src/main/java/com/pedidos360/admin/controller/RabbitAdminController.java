package com.pedidos360.admin.controller;

import com.pedidos360.admin.dto.CrearBindingRequest;
import com.pedidos360.admin.dto.CrearColaRequest;
import com.pedidos360.admin.dto.CrearExchangeRequest;
import com.pedidos360.admin.service.QueueInfo;
import com.pedidos360.admin.service.RabbitAdminService;
import jakarta.validation.Valid;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

// Rúbricas 6 y 7: endpoints de alto nivel; todo el detalle RabbitMQ
// está en RabbitAdminService.
@RestController
@RequestMapping("/api/rabbitmq")
public class RabbitAdminController {

  private final RabbitAdminService admin;

  public RabbitAdminController(RabbitAdminService admin) {
    this.admin = admin;
  }

  @PostMapping("/queues")
  public ResponseEntity<Map<String, String>> crearCola(@Valid @RequestBody CrearColaRequest req) {
    String nombre = admin.crearCola(req.nombre().trim(), req.durable());
    return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("nombre", nombre));
  }

  @PostMapping("/exchanges")
  public ResponseEntity<Map<String, String>> crearExchange(
      @Valid @RequestBody CrearExchangeRequest req) {
    String nombre = admin.crearExchange(req.nombre().trim(), req.tipo());
    return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("nombre", nombre));
  }

  @PostMapping("/bindings")
  public ResponseEntity<Void> crearBinding(@Valid @RequestBody CrearBindingRequest req) {
    String clave =
        req.routingKey() == null || req.routingKey().isBlank() ? null : req.routingKey().trim();
    admin.crearBinding(req.cola().trim(), req.exchange().trim(), clave);
    return ResponseEntity.status(HttpStatus.CREATED).build();
  }

  @DeleteMapping("/bindings")
  public ResponseEntity<Void> eliminarBinding(@Valid @RequestBody CrearBindingRequest req) {
    String clave =
        req.routingKey() == null || req.routingKey().isBlank() ? null : req.routingKey().trim();
    admin.eliminarBinding(req.cola().trim(), req.exchange().trim(), clave);
    return ResponseEntity.noContent().build();
  }

  @GetMapping("/queues/{nombre}")
  public QueueInfo infoCola(@PathVariable String nombre) {
    return admin.infoCola(nombre);
  }

  @DeleteMapping("/queues/{nombre}")
  public ResponseEntity<Void> eliminarCola(@PathVariable String nombre) {
    admin.eliminarCola(nombre);
    return ResponseEntity.noContent().build();
  }

  @DeleteMapping("/exchanges/{nombre}")
  public ResponseEntity<Void> eliminarExchange(@PathVariable String nombre) {
    admin.eliminarExchange(nombre);
    return ResponseEntity.noContent().build();
  }

  @PostMapping("/queues/{nombre}/purge")
  public Map<String, Integer> purgarCola(@PathVariable String nombre) {
    return Map.of("eliminados", admin.purgarCola(nombre));
  }
}
