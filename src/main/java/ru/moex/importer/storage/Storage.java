package ru.moex.importer.storage;

import ru.moex.importer.data.DataElement;

import java.util.List;

public interface Storage<T extends DataElement> {

    void batchInsertElements(List<T> elements);

    void insertSingleElement(T element);

    Integer getTableRowCnt();

    Integer getTableRowCntByCondition(String column, String expr);

    Long getTableMaxId();

}
