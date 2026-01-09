-- url: mysql://myuser:mypassword@localhost:3306/mydb

-- Example: Basic queries
SHOW TABLES;

SELECT * FROM users LIMIT 10;

SELECT id, name, email 
FROM users 
WHERE created_at > '2024-01-01';
