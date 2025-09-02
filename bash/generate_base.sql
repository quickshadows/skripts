-- Создать БД и настройки
CREATE DATABASE IF NOT EXISTS heavy_load CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE heavy_load;

-- Рекомендация: InnoDB, ROW_FORMAT=DYNAMIC для больших полей
SET GLOBAL innodb_file_per_table = ON;

-- Таблица пользователей — varchar, JSON профиля, enum, generated column
DROP TABLE IF EXISTS users;
CREATE TABLE users (
  user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  username VARCHAR(64) NOT NULL,
  email VARCHAR(192) NOT NULL,
  pwd_hash VARBINARY(128) NOT NULL,
  role ENUM('user','mod','admin','svc') NOT NULL DEFAULT 'user',
  signup_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_login TIMESTAMP NULL DEFAULT NULL,
  profile JSON,
  search_name VARCHAR(255) GENERATED ALWAYS AS (LOWER(username)) VIRTUAL,
  PRIMARY KEY (user_id),
  UNIQUE KEY (email),
  KEY (signup_ts),
  FULLTEXT KEY ft_username (username)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Таблица posts: large TEXT, FULLTEXT, JSON metadata, FK -> users
DROP TABLE IF EXISTS posts;
CREATE TABLE posts (
  post_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id BIGINT UNSIGNED NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  title VARCHAR(255) NOT NULL,
  body LONGTEXT NOT NULL,
  metadata JSON, -- tags, reactions, etc
  is_published TINYINT(1) NOT NULL DEFAULT 1,
  views BIGINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (post_id),
  KEY (user_id),
  KEY (created_at),
  FULLTEXT KEY ft_title_body (title, body),
  CONSTRAINT fk_posts_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Comments: many small rows, FK to posts/users, index on post and created_at
DROP TABLE IF EXISTS comments;
CREATE TABLE comments (
  comment_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  post_id BIGINT UNSIGNED NOT NULL,
  user_id BIGINT UNSIGNED NOT NULL,
  text TEXT NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  parent_comment_id BIGINT UNSIGNED DEFAULT NULL,
  upvotes INT DEFAULT 0,
  PRIMARY KEY (comment_id),
  KEY (post_id, created_at),
  KEY (user_id),
  CONSTRAINT fk_comments_post FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE,
  CONSTRAINT fk_comments_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE SET NULL
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Attachments: BLOB heavy table (store large binary data)
DROP TABLE IF EXISTS attachments;
CREATE TABLE attachments (
  attach_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  post_id BIGINT UNSIGNED DEFAULT NULL,
  filename VARCHAR(255),
  mime VARCHAR(100),
  file_blob LONGBLOB,
  file_size BIGINT UNSIGNED DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (attach_id),
  KEY (post_id),
  CONSTRAINT fk_attach_post FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE SET NULL
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Products as example with JSON spec + spatial location
DROP TABLE IF EXISTS products;
CREATE TABLE products (
  product_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  sku VARCHAR(64) NOT NULL,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  spec JSON, -- nested product spec
  price DECIMAL(12,2) NOT NULL DEFAULT 0,
  stock INT NOT NULL DEFAULT 0,
  location POINT, -- spatial column
  PRIMARY KEY (product_id),
  SPATIAL KEY (location),
  UNIQUE KEY (sku)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Orders and order_items (to generate join-heavy queries)
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  order_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id BIGINT UNSIGNED NOT NULL,
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  total DECIMAL(14,2) NOT NULL DEFAULT 0,
  status ENUM('new','paid','shipped','cancelled','returned') NOT NULL DEFAULT 'new',
  payload JSON,
  PRIMARY KEY (order_id),
  KEY (user_id, created_at),
  CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE SET NULL
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

DROP TABLE IF EXISTS order_items;
CREATE TABLE order_items (
  item_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  order_id BIGINT UNSIGNED NOT NULL,
  product_id BIGINT UNSIGNED NOT NULL,
  qty INT NOT NULL DEFAULT 1,
  price DECIMAL(12,2) NOT NULL DEFAULT 0,
  PRIMARY KEY (item_id),
  KEY (order_id),
  KEY (product_id),
  CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
  CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE RESTRICT
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Time-series metrics table (partitioned by month) — горячая точка записи
DROP TABLE IF EXISTS metrics;
CREATE TABLE metrics (
  metric_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  series VARCHAR(128) NOT NULL,
  ts DATETIME NOT NULL,
  value DOUBLE NOT NULL,
  tags JSON,
  PRIMARY KEY (metric_id, ts),
  KEY (series, ts)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC
-- пример партицирования (создавать партиции отдельно)
;

-- Large log table (append-only), партицирование по RANGE of YEAR(ts) рекомендуется
DROP TABLE IF EXISTS logs;
CREATE TABLE logs (
  log_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ts DATETIME NOT NULL,
  level ENUM('DEBUG','INFO','WARN','ERROR','CRIT') NOT NULL,
  service VARCHAR(128),
  message LONGTEXT,
  context JSON,
  PRIMARY KEY (log_id),
  KEY (ts),
  KEY (service, ts)
) ENGINE=InnoDB ROW_FORMAT=DYNAMIC;

-- Tagging many-to-many
DROP TABLE IF EXISTS tags;
CREATE TABLE tags (
  tag_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  name VARCHAR(64) NOT NULL,
  PRIMARY KEY (tag_id),
  UNIQUE KEY (name)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS post_tags;
CREATE TABLE post_tags (
  post_id BIGINT UNSIGNED NOT NULL,
  tag_id INT UNSIGNED NOT NULL,
  PRIMARY KEY (post_id, tag_id),
  KEY (tag_id),
  CONSTRAINT fk_pt_post FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE,
  CONSTRAINT fk_pt_tag FOREIGN KEY (tag_id) REFERENCES tags(tag_id) ON DELETE CASCADE
) ENGINE=InnoDB;
