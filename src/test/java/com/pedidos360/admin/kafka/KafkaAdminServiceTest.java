package com.pedidos360.admin.kafka;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class KafkaAdminServiceTest {
  private final KafkaAdminService service = new KafkaAdminService(null);

  @Test
  void impideEliminarElTopicDePagos() {
    assertThatThrownBy(() -> service.eliminar("p360.pagos.v1"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("protegido");
  }

  @Test
  void impideCambiarConfiguracionDeTopicsInternos() {
    assertThatThrownBy(() -> service.actualizar("__consumer_offsets", null, 60000L, "delete"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("protegido");
  }

  @Test
  void noPermiteBorrarOffsetsDeGruposDeNegocio() {
    assertThatThrownBy(() -> service.eliminarGrupo("p360-metricas-v1"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("protegido");
  }

  @Test
  void exigeAlMenosUnaOpcionParaActualizar() {
    assertThatThrownBy(() -> service.actualizar("p360.demo.prueba", null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cambio");
  }
}
