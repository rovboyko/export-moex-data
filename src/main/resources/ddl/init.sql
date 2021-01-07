CREATE TABLE trades
(
    tradeno Int64,
    boardname String,
    secid String,
    tradedate Date,
    tradetime Datetime,
    price Decimal(18,8),
    quantity Int32,
    systime Datetime

) ENGINE = ReplacingMergeTree()
PARTITION BY tradedate
ORDER BY (secid, tradedate, tradeno)
SAMPLE BY (secid, tradedate);

CREATE TABLE candles_1m
(
    secid String,
    open Decimal(18,8),
    close Decimal(18,8),
    high Decimal(18,8),
    low Decimal(18,8),
    value Decimal(18,8),
    volume Decimal (18,8),
    begin Datetime,
    end Datetime
) ENGINE = ReplacingMergeTree()
PARTITION BY toDate(begin)
ORDER BY (secid, begin)
SAMPLE BY (secid, begin);

CREATE TABLE candles_10m
(
    secid String,
    open Decimal(18,8),
    close Decimal(18,8),
    high Decimal(18,8),
    low Decimal(18,8),
    value Decimal(18,8),
    volume Decimal (18,8),
    begin Datetime,
    end Datetime
) ENGINE = ReplacingMergeTree()
PARTITION BY toDate(begin)
ORDER BY (secid, begin)
SAMPLE BY (secid, begin);

CREATE TABLE candles_24h
(
    secid String,
    open Decimal(18,8),
    close Decimal(18,8),
    high Decimal(18,8),
    low Decimal(18,8),
    value Decimal(18,8),
    volume Decimal (18,8),
    begin Datetime,
    end Datetime
) ENGINE = ReplacingMergeTree()
PARTITION BY toDate(begin)
ORDER BY (secid, begin)
SAMPLE BY (secid, begin);