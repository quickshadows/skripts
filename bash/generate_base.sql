-- Создание базы данных
CREATE DATABASE IF NOT EXISTS LargeDB;
USE LargeDB;

-- Таблица пользователей
CREATE TABLE Users (
    UserID INT AUTO_INCREMENT PRIMARY KEY,
    Username VARCHAR(50) NOT NULL,
    Email VARCHAR(100) NOT NULL,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица продуктов
CREATE TABLE Products (
    ProductID INT AUTO_INCREMENT PRIMARY KEY,
    ProductName VARCHAR(100) NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    Stock INT NOT NULL
);

-- Таблица заказов
CREATE TABLE Orders (
    OrderID INT AUTO_INCREMENT PRIMARY KEY,
    UserID INT,
    OrderDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (UserID) REFERENCES Users(UserID)
);

-- Таблица деталей заказа
CREATE TABLE OrderDetails (
    OrderDetailID INT AUTO_INCREMENT PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    Quantity INT NOT NULL,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID),
    FOREIGN KEY (ProductID) REFERENCES Products(ProductID)
);

-- Дополнительная таблица для генерации объема данных
CREATE TABLE LargeData (
    DataID INT AUTO_INCREMENT PRIMARY KEY,
    RandomText TEXT,
    RandomNumber BIGINT
);

-- Заполнение таблицы пользователей
INSERT INTO Users (Username, Email) 
SELECT CONCAT('user', n), CONCAT('user', n, '@example.com')
FROM (
    SELECT @rownum := @rownum + 1 AS n FROM 
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
    (SELECT @rownum := 0) t3
) AS numbers
LIMIT 100000;  -- 100,000 пользователей

-- Заполнение таблицы продуктов
INSERT INTO Products (ProductName, Price, Stock) 
SELECT 
    CONCAT('Product ', n), 
    ROUND(RAND() * 100, 2), 
    FLOOR(RAND() * 1000)
FROM (
    SELECT @rownum := @rownum + 1 AS n FROM 
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
    (SELECT @rownum := 0) t3
) AS numbers
LIMIT 100000;  -- 100,000 продуктов

-- Заполнение таблицы заказов
INSERT INTO Orders (UserID) 
SELECT FLOOR(1 + RAND() * 100000) 
FROM (
    SELECT @rownum := @rownum + 1 AS n FROM 
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
    (SELECT @rownum := 0) t3
) AS numbers
WHERE FLOOR(1 + RAND() * 100000) <= (SELECT COUNT(*) FROM Users)  -- Убедитесь, что UserID существует
LIMIT 200000;  -- 200,000 заказов

-- Заполнение таблицы деталей заказа
INSERT INTO OrderDetails (OrderID, ProductID, Quantity) 
SELECT 
    FLOOR(1 + RAND() * 200000), 
    FLOOR(1 + RAND() * 100000), 
    FLOOR(1 + RAND() * 10) 
FROM (
    SELECT @rownum := @rownum + 1 AS n FROM 
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
    (SELECT @rownum := 0) t3
) AS numbers
LIMIT 500000;  -- 500,000 деталей заказа

-- Заполнение таблицы LargeData для увеличения объема
INSERT INTO LargeData (RandomText, RandomNumber) 
SELECT 
    REPEAT('Random text ', 50), 
    FLOOR(RAND() * 1000000000) 
FROM (
    SELECT @rownum := @rownum + 1 AS n FROM 
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t1,
    (SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) t2,
    (SELECT @rownum := 0) t3
) AS numbers
LIMIT 200000;  -- 200,000 записей для увеличения объема
