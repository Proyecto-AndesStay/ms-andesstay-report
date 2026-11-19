package com.andesstay.report.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReportKafkaConsumerServiceTest {

    private ReportKafkaConsumerService service;

    @BeforeEach
    void setUp() {
        service = new ReportKafkaConsumerService();
    }

    @Test
    void consumeStreamingEvent_DeberiaActualizarMetricasCorrectamente() {
        service.consumeStreamingEvent("{\"eventType\":\"RESERVATION_CREATED\",\"status\":\"CREADA\"}", 0, 1L);
        service.consumeStreamingEvent("{\"eventType\":\"STATUS_UPDATED\",\"status\":\"CONFIRMADA\"}", 0, 2L);
        service.consumeStreamingEvent("{\"eventType\":\"STATUS_UPDATED\",\"status\":\"EN_ESTADIA\"}", 1, 3L);
        service.consumeStreamingEvent("{\"eventType\":\"RESERVATION_DELETED\",\"status\":\"CANCELADA\"}", 2, 4L);

        Map<String, Object> kpis = service.getRealtimeKpis();

        assertEquals(4, kpis.get("totalEventosStreaming"));
        assertEquals(1, kpis.get("reservasCreadas"));
        assertEquals(1, kpis.get("reservasConfirmadas"));
        assertEquals(1, kpis.get("huespedesEnEstadia"));
        assertEquals(1, kpis.get("reservasEliminadas"));
        assertEquals(10.0, (Double) kpis.get("ocupacionPorcentaje"), 0.01);
    }

    @Test
    void consumeStreamingEvent_DeberiaIgnorarMensajesNulosOVacios() {
        service.consumeStreamingEvent(null, 0, 1L);
        service.consumeStreamingEvent("   ", 0, 2L);

        Map<String, Object> kpis = service.getRealtimeKpis();

        assertEquals(0, kpis.get("totalEventosStreaming"));
        assertEquals(85.4, (Double) kpis.get("ocupacionPorcentaje"), 0.01);
    }

    @Test
    void consumeStreamingEvent_NoDeberiaLanzarExcepcionConJsonInvalido() {
        assertDoesNotThrow(() -> service.consumeStreamingEvent("{json-roto", 0, 10L));

        Map<String, Object> kpis = service.getRealtimeKpis();
        assertEquals(0, kpis.get("totalEventosStreaming"));
    }
}