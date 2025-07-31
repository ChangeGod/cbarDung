create table  market_data (
    symbol VARCHAR(20) not null ,
    expiration_date VARCHAR(20) not null ,
    update_time datetime not null,
    PRIMARY KEY (symbol, expiration_date)
);
--
CREATE TABLE symbol_list (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            ticker VARCHAR(10) NOT NULL,
                            company VARCHAR(255),
                            sector VARCHAR(100),
                            industry VARCHAR(100),
                            country VARCHAR(100)
);

