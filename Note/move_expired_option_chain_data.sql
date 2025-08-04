SET GLOBAL event_scheduler = ON;

CREATE EVENT IF NOT EXISTS move_expired_option_chain_data
    ON SCHEDULE EVERY 1 DAY
        STARTS TIMESTAMP(CURRENT_DATE, '00:10:00')
    DO
    BEGIN
        -- Move expired option chain data
        INSERT INTO market_archive.option_chain_data_archive
        (symbol, Cycle_Range, expiration_date, expiration_type, update_date, update_time,
         contract_type, strike, moneyness, bid, mid, ask, last,
         theoretical, change_val, pct_chg, volume, open_interest, oi_change,
         vol_oi_ratio, iv, delta, gamma, theta, vega, rho,
         itm_probability, time_quoted)
        SELECT
            symbol, Cycle_Range, expiration_date, expiration_type, update_date, update_time,
            contract_type, strike, moneyness, bid, mid, ask, last,
            theoretical, change_val, pct_chg, volume, open_interest, oi_change,
            vol_oi_ratio, iv, delta, gamma, theta, vega, rho,
            itm_probability, time_quoted
        FROM your_main_db.option_chain_data
        WHERE STR_TO_DATE(expiration_date, '%Y-%m-%d') < CURDATE();

        -- Delete expired data from main table
        DELETE FROM your_main_db.option_chain_data
        WHERE STR_TO_DATE(expiration_date, '%Y-%m-%d') < CURDATE();
    END;
