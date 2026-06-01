-- dbtype: mysql
-- host: db.example.local
-- port: 3306
-- user: app_user
-- database: app_db

SHOW TABLES;

SELECT * FROM users LIMIT 10;

SELECT id, name, email
FROM users
WHERE created_at > '2024-01-01';
