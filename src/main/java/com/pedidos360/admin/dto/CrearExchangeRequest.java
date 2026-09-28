package com.pedidos360.admin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record CrearExchangeRequest(
    @NotBlank(message = "El nombre del exchange es obligatorio.") String nombre,
    @NotBlank(message = "El tipo de exchange es obligatorio.")
        @Pattern(
            regexp = "(?i)direct|topic|fanout",
            message = "Tipo inválido. Válidos: direct, topic, fanout.")
        String tipo) {}
