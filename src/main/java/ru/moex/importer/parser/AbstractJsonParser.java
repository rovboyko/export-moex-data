package ru.moex.importer.parser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import ru.moex.importer.data.DataElement;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public abstract class AbstractJsonParser<T extends DataElement> {

    static final DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static final DateTimeFormatter dFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    static final DateTimeFormatter tFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    protected final ObjectMapper mapper = new ObjectMapper();
    protected final JsonNode rootNode;

    public AbstractJsonParser(String json) {
        try {
            rootNode = mapper.readTree(json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(String.format("can't parse json: %s", json));
        }
    }

    public List<T> getDataElements() {
        var jsonData = (ArrayNode) getJsonData();
        var dataElements = new ArrayList<T>();

        var elements = jsonData.elements();
        while(elements.hasNext()) {
            dataElements.add(deserializeDataElement(elements.next()));
        }
        return dataElements;
    }

    public Optional<T> getFirstDataElement() {
        var jsonData = (ArrayNode) getJsonData();
        var elements = jsonData.elements();
        if (elements.hasNext()) {
            return Optional.of(deserializeDataElement(elements.next()));
        } else {
            return Optional.empty();
        }
    }

    abstract JsonNode getJsonData();

    abstract T deserializeDataElement(JsonNode jsonNode);
}
