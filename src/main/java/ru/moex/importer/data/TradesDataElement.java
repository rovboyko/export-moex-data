package ru.moex.importer.data;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

@Data
@Builder
public class TradesDataElement {

    private Long tradeNo;
    private String boardName;
    private String secId;
    private LocalDate tradeDate;
    private LocalTime tradeTime;
    private Double price;
    private Integer quantity;
    private LocalDateTime sysTime;

}
