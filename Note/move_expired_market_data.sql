SET GLOBAL event_scheduler = ON;

CREATE EVENT IF NOT EXISTS move_expired_market_data
    ON SCHEDULE EVERY 1 DAY
        STARTS TIMESTAMP(CURRENT_DATE, '00:00:00')
    DO
    BEGIN
        INSERT INTO market_archive.market_data_archive
        SELECT *
        FROM your_main_db.market_data
        WHERE STR_TO_DATE(LEFT(expiration_date, 10), '%Y-%m-%d') < CURDATE();

        DELETE FROM your_main_db.market_data
        WHERE STR_TO_DATE(LEFT(expiration_date, 10), '%Y-%m-%d') < CURDATE();
    END;
