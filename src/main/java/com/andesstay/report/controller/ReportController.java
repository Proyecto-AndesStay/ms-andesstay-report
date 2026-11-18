package com.andesstay.report.controller;

import com.andesstay.report.service.ReportKafkaConsumerService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/reports")
@CrossOrigin(origins = "*")
public class ReportController {

    private final ReportKafkaConsumerService reportService;

    public ReportController(ReportKafkaConsumerService reportService) {
        this.reportService = reportService;
    }

    @GetMapping("/kpis")
    public Map<String, Object> getKpis() {
        return reportService.getRealtimeKpis();
    }
}