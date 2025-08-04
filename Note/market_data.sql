CREATE TABLE market_data
(
    symbol              VARCHAR(20)    NOT NULL,
    Cycle_Range         VARCHAR(50)    NOT NULL,   -- changed here
    expiration_date     VARCHAR(20)    NOT NULL,
    update_date         DATE           NOT NULL,
    update_time         TIME           NOT NULL,
    DTE                 INT            NULL,
    Put_Vol             INT            NULL,
    Call_Vol            INT            NULL,
    Total_Vol           INT            NULL,
    Put_or_Call_Vol     DECIMAL(10, 4) NULL,
    Put_OI              INT            NULL,
    Call_OI             INT            NULL,
    Total_OI            INT            NULL,
    Put_or_Call_OI      DECIMAL(10, 4) NULL,
    IV                  DECIMAL(10, 4) NULL,
    Historic_Volatility DECIMAL(10, 4) NULL,
    IV_Rank             DECIMAL(10, 4) NULL,
    IV_Percentile       DECIMAL(10, 4) NULL,
    PRIMARY KEY (symbol, Cycle_Range, expiration_date, update_date)
);
