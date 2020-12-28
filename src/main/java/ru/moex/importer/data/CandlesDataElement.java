package ru.moex.importer.data;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Data
@Builder
public class CandlesDataElement {

    private String secId;
    private Double open;
    private Double close;
    private Double high;
    private Double low;
    private Double value;
    private Double volume;
    private LocalDateTime begin;
    private LocalDateTime end;
}
