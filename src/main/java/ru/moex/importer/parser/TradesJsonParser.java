package ru.moex.importer.parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import ru.moex.importer.data.TradesDataElement;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class TradesJsonParser extends AbstractJsonParser<TradesDataElement> {

    public TradesJsonParser(String json) {
       super(json);
    }

    @Override
    JsonNode getJsonData() {
        return rootNode.get("trades").get("data");
    }

    @Override
    TradesDataElement deserializeDataElement(JsonNode jsonNode) {
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
