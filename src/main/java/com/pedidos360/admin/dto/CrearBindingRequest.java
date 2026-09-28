package com.pedidos360.admin.dto;

import jakarta.validation.constraints.NotBlank;

public record CrearBindingRequest(
    @NotBlank(message = "La cola es obligatoria.") String cola,
    @NotBlank(message = "El exchange es obligatorio.") String exchange,
    // Opcional: en fanout se ignora.
    String routingKey) {}
