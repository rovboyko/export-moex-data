package ru.moex.importer.data;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder
public class CandlesDataElement implements DataElement {

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
