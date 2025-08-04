CREATE TABLE market_data_archive
(
    symbol          VARCHAR(20)    NOT NULL,
    Cycle_Range     VARCHAR(50)    NOT NULL,
    expiration_date VARCHAR(20)    NOT NULL,
    expiration_type VARCHAR(20)    NOT NULL,
    update_date     DATE           NOT NULL,
    update_time     TIME           NOT NULL,
    DTE             INT            NULL,
    Put_Vol         INT            NULL,
    Call_Vol        INT            NULL,
    Total_Vol       INT            NULL,
    Put_or_Call_Vol DECIMAL(10, 4) NULL,
    Put_OI          INT            NULL,
    Call_OI         INT            NULL,
    Total_OI        INT            NULL,
    Put_or_Call_OI  DECIMAL(10, 4) NULL,
    IV              DECIMAL(10, 4) NULL,
    PRIMARY KEY (symbol, Cycle_Range, expiration_date, expiration_type, update_date)
);
