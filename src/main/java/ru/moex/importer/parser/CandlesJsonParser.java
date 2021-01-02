package ru.moex.importer.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import ru.moex.importer.data.CandlesDataElement;
import ru.moex.importer.data.TradesDataElement;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CandlesJsonParser extends AbstractJsonParser<CandlesDataElement> {

    private final String secId;

    public CandlesJsonParser(String json, String secId) {
        super(json);
        this.secId = secId;
    }

    @Override
    JsonNode getJsonData() {
        return rootNode.get("candles").get("data");
    }

    @Override
    CandlesDataElement deserializeDataElement(JsonNode jsonNode) {
        if (!(jsonNode instanceof ArrayNode)) {
            throw new RuntimeException(String.format("can't convert trades element: %s", jsonNode));
        }
        var fields = jsonNode.elements();
        var builder = CandlesDataElement.builder();
        builder.secId(this.secId);
        var i = 0;
        while(fields.hasNext()) {
            if (i == 0) builder.open(fields.next().asDouble());
            else if (i == 1) builder.close(fields.next().asDouble());
            else if (i == 2) builder.high(fields.next().asDouble());
            else if (i == 3) builder.low(fields.next().asDouble());
            else if (i == 4) builder.value(fields.next().asDouble());
            else if (i == 5) builder.volume(fields.next().asDouble());
            else if (i == 6) builder.begin(LocalDateTime.parse(fields.next().textValue(), dtFormatter));
            else if (i == 7) builder.end(LocalDateTime.parse(fields.next().textValue(), dtFormatter));
            else {
                throw new RuntimeException(String.format("can't convert trades element: %s unexpected element with number %s",
                        jsonNode, i));
            }
            i++;
        }
        return builder.build();
    }
}
