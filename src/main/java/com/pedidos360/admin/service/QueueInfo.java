package com.pedidos360.admin.service;

// Lo que devuelve GET /queues/{nombre}.
public record QueueInfo(String nombre, int mensajes, int consumidores) {}
