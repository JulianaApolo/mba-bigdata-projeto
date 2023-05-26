CREATE TABLE COINS(
    ID VARCHAR2(100),
    NAME VARCHAR2(100),
    IMAGE VARCHAR2(200),
    CREATION_DATE TIMESTAMP,
    UPDATED_DATE  TIMESTAMP,
    PRIMARY KEY(id)
);

CREATE TABLE QUOTES(
    ID INTEGER GENERATED BY DEFAULT AS IDENTITY,
    HISTORICAL_DATE DATE,
    CRYPTO_ID VARCHAR2(100),
    OPEN_VALUE NUMBER,
    CLOSE_VALUE NUMBER,
    HIGH_VALUE NUMBER,
    LOW_VALUE NUMBER,
    VOLUME NUMBER,
    MARKET_CAP NUMBER,
    CREATION_DATE TIMESTAMP,
    PRIMARY KEY(id),
    FOREIGN KEY(CRYPTO_ID) REFERENCES COINS(ID)
);