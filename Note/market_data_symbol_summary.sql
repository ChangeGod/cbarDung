CREATE OR REPLACE VIEW market_data_symbol_summary AS
SELECT
    symbol,
    Cycle_Range,
    SUM(Put_Vol) AS put_volume_total,
    SUM(Call_Vol) AS call_volume_total,
    CASE
        WHEN SUM(Call_Vol) = 0 THEN NULL
        ELSE ROUND(SUM(Put_Vol) / SUM(Call_Vol), 4)
        END AS put_call_volume_ratio,
    SUM(Put_OI) AS put_open_interest_total,
    SUM(Call_OI) AS call_open_interest_total,
    CASE
        WHEN SUM(Call_OI) = 0 THEN NULL
        ELSE ROUND(SUM(Put_OI) / SUM(Call_OI), 4)
        END AS put_call_open_interest_ratio,
    MAX(IV) AS IV,
    MAX(Historic_Volatility) AS Historic_Volatility,
    MAX(IV_Rank) AS IV_Rank,
    MAX(IV_Percentile) AS IV_Percentile
FROM market_data
GROUP BY symbol, Cycle_Range;
