CREATE DATABASE IF NOT EXISTS gen10g CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE gen10g;

-- Таблица ~1.2 КБ на строку (в среднем)
DROP TABLE IF EXISTS fat_rows;
CREATE TABLE fat_rows (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  name VARCHAR(64) NOT NULL,
  email VARCHAR(96) NOT NULL,
  city VARCHAR(64) NOT NULL,
  bio TEXT,                       -- ~200–400 байт в среднем (переменно)
  payload VARBINARY(1024) NOT NULL, -- основная масса (до 1 КБ)
  PRIMARY KEY (id),
  KEY (created_at)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

SET SESSION innodb_flush_log_at_trx_commit = 2;
SET SESSION unique_checks = 0;
SET SESSION foreign_key_checks = 0;
SET SESSION cte_max_recursion_depth = 1000000;
SET SESSION sql_log_bin = 0; -- если это не реплика/прод
SET autocommit = 0;

-- Сколько батчей хотим (100k * 90 = 9 млн строк):
SET @batches := 90;

-- Один батч = 100 000 строк из рекурсивного CTE
-- В payload заполняем ~1 КБ, в текстовые поля даём немного случайности.
DROP PROCEDURE IF EXISTS fill_fat_rows;
DELIMITER $$
CREATE PROCEDURE fill_fat_rows(IN batches INT)
BEGIN
  DECLARE i INT DEFAULT 1;
  WHILE i <= batches DO
    WITH RECURSIVE seq AS (
      SELECT 1 AS n
      UNION ALL
      SELECT n + 1 FROM seq WHERE n < 100000
    )
    INSERT INTO fat_rows (name, email, city, bio, payload)
    SELECT
      CONCAT('user_', UUID()) AS name,
      CONCAT(UUID(), '@example.com') AS email,
      ELT(FLOOR(1 + RAND()*6),
          'Amsterdam','Berlin','Warsaw','Prague','Madrid','Rome') AS city,
      RPAD(CONCAT('bio ', UUID(), ' ', UUID()), 300, 'x') AS bio,
      -- ~1 КБ «шума»: случайный хеш + добивка до 1024 байт
      CAST(RPAD(SHA2(UUID(), 256), 1024, '0') AS VARBINARY(1024)) AS payload
    FROM seq;

    COMMIT; -- фиксируем батч, освобождаем redo pressure
    SET i = i + 1;
  END WHILE;
END$$
DELIMITER ;

CALL fill_fat_rows(@batches);


SET autocommit = 1;
SET SESSION unique_checks = 1;
SET SESSION foreign_key_checks = 1;
SET SESSION innodb_flush_log_at_trx_commit = 1;
