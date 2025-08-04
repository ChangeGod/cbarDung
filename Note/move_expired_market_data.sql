CREATE EVENT IF NOT EXISTS move_expired_market_data
    ON SCHEDULE EVERY 1 DAY
        STARTS TIMESTAMP(CURRENT_DATE, '00:00:00')
    DO
    BEGIN
        -- Move expired records to archive
        INSERT INTO market_archive.market_data_archive
        (symbol, Cycle_Range, expiration_date, expiration_type, update_date, update_time,
         DTE, Put_Vol, Call_Vol, Total_Vol, Put_or_Call_Vol,
         Put_OI, Call_OI, Total_OI, Put_or_Call_OI, IV,
         Historic_Volatility, IV_Rank, IV_Percentile,
         Base_LastPrice, Implied_Move, Implied_Move_Percent, Base_Upper_Price, Base_Lower_Price)
        SELECT
            symbol, Cycle_Range, expiration_date, expiration_type, update_date, update_time,
            DTE, Put_Vol, Call_Vol, Total_Vol, Put_or_Call_Vol,
            Put_OI, Call_OI, Total_OI, Put_or_Call_OI, IV,
            Historic_Volatility, IV_Rank, IV_Percentile,
            Base_LastPrice, Implied_Move, Implied_Move_Percent, Base_Upper_Price, Base_Lower_Price
        FROM your_main_db.market_data
        WHERE STR_TO_DATE(expiration_date, '%Y-%m-%d') < CURDATE();

        -- Delete moved rows from main table
        DELETE FROM your_main_db.market_data
        WHERE STR_TO_DATE(expiration_date, '%Y-%m-%d') < CURDATE();
    END;
