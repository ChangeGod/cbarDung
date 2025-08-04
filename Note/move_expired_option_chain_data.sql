SET GLOBAL event_scheduler = ON;

CREATE EVENT IF NOT EXISTS move_expired_option_chain_data
    ON SCHEDULE EVERY 1 DAY
        STARTS TIMESTAMP(CURRENT_DATE, '00:10:00') -- 00:10 AM every day
    DO
    BEGIN
        -- Move expired option chain contracts
        INSERT INTO market_archive.option_chain_data_archive
        (symbol, Cycle_Range, expiration_date, update_date, update_time,
         contract_type, strike, moneyness, bid, mid, ask, last,
         change_val, pct_chg, volume, open_interest, oi_change,
         iv, delta, time_quoted, links)
        SELECT
            symbol, Cycle_Range, expiration_date, update_date, update_time,
            contract_type, strike, moneyness, bid, mid, ask, last,
            change_val, pct_chg, volume, open_interest, oi_change,
            iv, delta, time_quoted, links
        FROM your_main_db.option_chain_data
        WHERE STR_TO_DATE(LEFT(expiration_date, 10), '%Y-%m-%d') < CURDATE();

        -- Delete moved contracts from main table
        DELETE FROM your_main_db.option_chain_data
        WHERE STR_TO_DATE(LEFT(expiration_date, 10), '%Y-%m-%d') < CURDATE();
    END;
