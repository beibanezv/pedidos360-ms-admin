package com.pedidos360.admin.dto;

import jakarta.validation.constraints.NotBlank;

// Rúbrica 8: el nombre vacío se rechaza con 400 antes de tocar el broker.
public record CrearColaRequest(
    @NotBlank(message = "El nombre de la cola es obligatorio.") String nombre,
    // Opcional. Si se omite, la cola se crea durable (sobrevive reinicios).
    Boolean durable) {}
