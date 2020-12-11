package ru.moex.importer.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import ru.moex.importer.data.TradesDataElement;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TradesJsonParser {

    private static final DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter dFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter tFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonNode rootNode;

    public TradesJsonParser(String json) {
        try {
            rootNode = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(String.format("can't parse json: %s", json));
        }
    }

    public List<TradesDataElement> getTradesData() {
        var jsonTradesData = (ArrayNode) rootNode.get("trades").get("data");
        var tradesData = new ArrayList<TradesDataElement>();

        var elements = jsonTradesData.elements();
        while(elements.hasNext()) {
            tradesData.add(deserializeDataElement(elements.next()));
        }
        return tradesData;
    }

    private TradesDataElement deserializeDataElement(JsonNode jsonNode) {
        if (!(jsonNode instanceof ArrayNode)) {
            throw new RuntimeException(String.format("can't convert trades element: %s", jsonNode));
        }
        var fields = jsonNode.elements();
        var builder = TradesDataElement.builder();
        var i = 0;
        while(fields.hasNext()) {
            if (i == 0) builder.tradeNo(fields.next().longValue());
            else if (i == 1) builder.boardName(fields.next().textValue());
            else if (i == 2) builder.secId(fields.next().textValue());
            else if (i == 3) builder.tradeDate(LocalDate.parse(fields.next().textValue(), dFormatter));
            else if (i == 4) builder.tradeTime(LocalTime.parse(fields.next().textValue(), tFormatter));
            else if (i == 5) builder.price(fields.next().asDouble());
            else if (i == 6) builder.quantity(fields.next().intValue());
            else if (i == 7) builder.sysTime(LocalDateTime.parse(fields.next().textValue(), dtFormatter));
            else {
                throw new RuntimeException(String.format("can't convert trades element: %s unexpected element with number %s",
                        jsonNode, i));
            }
            i++;
        }
        return builder.build();
    }
}
