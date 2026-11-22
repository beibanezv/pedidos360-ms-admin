package com.pedidos360.admin.kafka;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
class KafkaAdminController {
  private final KafkaAdminService admin;

  KafkaAdminController(KafkaAdminService admin) {
    this.admin = admin;
  }

  @GetMapping("/topics")
  List<String> topics() { return admin.topics(); }

  @GetMapping("/topics/{nombre}")
  KafkaAdminService.Tema topic(@PathVariable String nombre) { return admin.tema(nombre); }

  @PostMapping("/topics")
  @ResponseStatus(HttpStatus.CREATED)
  void crear(@Valid @RequestBody CrearTopic req) {
    admin.crear(req.nombre(), req.particiones(), req.replicas(), req.retencionMs(), req.limpieza());
  }

  @PatchMapping("/topics/{nombre}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  void actualizar(@PathVariable String nombre, @Valid @RequestBody ActualizarTopic req) {
    admin.actualizar(nombre, req.particiones(), req.retencionMs(), req.limpieza());
  }

  @DeleteMapping("/topics/{nombre}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  void eliminar(@PathVariable String nombre) { admin.eliminar(nombre); }

  @GetMapping("/groups")
  List<String> grupos() { return admin.grupos(); }

  @GetMapping("/groups/{nombre}")
  KafkaAdminService.Grupo grupo(@PathVariable String nombre) { return admin.grupo(nombre); }

  @DeleteMapping("/groups/{nombre}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  void eliminarGrupo(@PathVariable String nombre) { admin.eliminarGrupo(nombre); }

  record CrearTopic(@NotBlank String nombre, @NotNull @Min(1) @Max(24) Integer particiones,
      @NotNull @Min(1) @Max(3) Short replicas, @NotNull @Min(60000) Long retencionMs,
      @NotBlank @Pattern(regexp = "delete|compact|compact,delete") String limpieza) {}

  record ActualizarTopic(@Min(1) @Max(24) Integer particiones, @Min(60000) Long retencionMs,
      @Pattern(regexp = "delete|compact|compact,delete") String limpieza) {}
}
