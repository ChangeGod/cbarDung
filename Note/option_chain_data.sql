CREATE TABLE option_chain_data
(
    symbol              VARCHAR(20)     NOT NULL, -- FK to market_data
    Cycle_Range         VARCHAR(50)     NOT NULL, -- FK to market_data
    expiration_date     VARCHAR(20)     NOT NULL, -- FK to market_data
    update_date         DATE            NOT NULL, -- FK to market_data
    update_time         TIME            NOT NULL, -- FK to market_data

    contract_type       ENUM('C','P')   NOT NULL, -- 'C' = Call, 'P' = Put
    strike              DECIMAL(10, 2)  NOT NULL,

    moneyness           VARCHAR(20)     NULL,
    bid                 DECIMAL(10, 2)  NULL,
    mid                 DECIMAL(10, 2)  NULL,
    ask                 DECIMAL(10, 2)  NULL,
    last                DECIMAL(10, 2)  NULL,
    change_val          DECIMAL(10, 2)  NULL,
    pct_chg             VARCHAR(20)     NULL,
    volume              INT             NULL,
    open_interest       INT             NULL,
    oi_change           VARCHAR(20)     NULL,
    iv                  DECIMAL(10, 2)  NULL,
    delta               DECIMAL(10, 4)  NULL,
    time_quoted         VARCHAR(20)     NULL,
    links               VARCHAR(255)    NULL,

    PRIMARY KEY (symbol, Cycle_Range, expiration_date, update_date, strike, contract_type),

    CONSTRAINT fk_option_chain_market
        FOREIGN KEY (symbol, Cycle_Range, expiration_date, update_date)
            REFERENCES market_data(symbol, Cycle_Range, expiration_date, update_date)
            ON DELETE CASCADE
);

-- Index for performance on symbol, expiration_date, contract_type
CREATE INDEX idx_option_chain_symbol_exp_contract
    ON option_chain_data(symbol, expiration_date, contract_type);
