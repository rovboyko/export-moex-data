package ru.moex.importer.data;

public enum CandlesInterval {
    ONE_MINUTE("1m"),
    TEN_MINUTES("10m"),
    ONE_DAY("24h");

    private final String value;

    CandlesInterval(String value){
        this.value = value;
    }

    public String getValue(){ return value;}

    public String getValueForRequest() {
        return value.replaceAll("[^0-9]","");
    }

    public boolean equalzz(String otherValue) {
        return value.equalsIgnoreCase(otherValue);
    }
}
