CREATE TABLE option_chain_data
(
    symbol            VARCHAR(20)     NOT NULL,
    Cycle_Range       VARCHAR(50)     NOT NULL,
    expiration_date   VARCHAR(20)     NOT NULL,
    expiration_type   VARCHAR(20)     NOT NULL,
    update_date       DATE            NOT NULL,
    update_time       TIME            NOT NULL,

    contract_type     ENUM('C','P')   NOT NULL,
    strike            DECIMAL(10, 2)  NOT NULL,

    -- Pricing and market data
    moneyness         VARCHAR(20)     NULL,
    bid               DECIMAL(10, 2)  NULL,
    mid               DECIMAL(10, 2)  NULL,
    ask               DECIMAL(10, 2)  NULL,
    last              DECIMAL(10, 2)  NULL,
    theoretical       DECIMAL(10, 4)  NULL,
    change_val        DECIMAL(10, 2)  NULL,
    pct_chg           VARCHAR(20)     NULL,

    -- Volume & Open Interest
    volume            INT             NULL,
    open_interest     INT             NULL,
    oi_change         VARCHAR(20)     NULL,
    vol_oi_ratio      DECIMAL(10, 4)  NULL,

    -- Volatility & Greeks
    iv                DECIMAL(10, 4)  NULL,
    delta             DECIMAL(10, 4)  NULL,
    gamma             DECIMAL(10, 4)  NULL,
    theta             DECIMAL(10, 4)  NULL,
    vega              DECIMAL(10, 4)  NULL,
    rho               DECIMAL(10, 4)  NULL,

    -- Probability & timing
    itm_probability   DECIMAL(10, 4)  NULL,
    time_quoted       VARCHAR(20)     NULL,

    PRIMARY KEY (symbol, Cycle_Range, expiration_date, expiration_type, update_date, strike, contract_type),

    CONSTRAINT fk_option_chain_market
        FOREIGN KEY (symbol, Cycle_Range, expiration_date, expiration_type, update_date)
            REFERENCES market_data(symbol, Cycle_Range, expiration_date, expiration_type, update_date)
            ON DELETE CASCADE
);

-- Index for performance
CREATE INDEX idx_option_chain_symbol_exp_contract
    ON option_chain_data(symbol, expiration_date, contract_type);
