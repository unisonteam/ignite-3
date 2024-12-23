create table CALL_CENTER
(
    CC_CALL_CENTER_SK INTEGER       not null,
    CC_CALL_CENTER_ID VARCHAR(16) not null,
    CC_REC_START_DATE DATE,
    CC_REC_END_DATE   DATE,
    CC_CLOSED_DATE_SK INTEGER,
    CC_OPEN_DATE_SK   INTEGER,
    CC_NAME           VARCHAR(50),
    CC_CLASS          VARCHAR(50),
    CC_EMPLOYEES      INTEGER,
    CC_SQ_FT          INTEGER,
    CC_HOURS          VARCHAR(20),
    CC_MANAGER        VARCHAR(40),
    CC_MKT_ID         INTEGER,
    CC_MKT_CLASS      VARCHAR(50),
    CC_MKT_DESC       VARCHAR(100),
    CC_MARKET_MANAGER VARCHAR(40),
    CC_DIVISION       INTEGER,
    CC_DIVISION_NAME  VARCHAR(50),
    CC_COMPANY        INTEGER,
    CC_COMPANY_NAME   VARCHAR(50),
    CC_STREET_NUMBER  VARCHAR(10),
    CC_STREET_NAME    VARCHAR(60),
    CC_STREET_TYPE    VARCHAR(15),
    CC_SUITE_NUMBER   VARCHAR(10),
    CC_CITY           VARCHAR(60),
    CC_COUNTY         VARCHAR(30),
    CC_STATE          VARCHAR(2),
    CC_ZIP            VARCHAR(10),
    CC_COUNTRY        VARCHAR(20),
    CC_GMT_OFFSET     NUMERIC(5, 2),
    CC_TAX_PERCENTAGE NUMERIC(5, 2),
    constraint CALL_CENTER_PK
        primary key (CC_CALL_CENTER_SK)
);
