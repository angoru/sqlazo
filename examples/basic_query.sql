-- url: mysql://root:root@localhost:3306/hostbill_bpg

-- Example: Basic queries
SHOW TABLES;

-- ERROR: Error: 1049 (42000): Unknown database 'hosbill_bpg' 



SELECT * FROM users LIMIT 10;

-- ERROR: Error: 1049 (42000): Unknown database 'hosbill_bpg' 



SELECT id, name, email 
FROM users 
WHERE created_at > '2024-01-01';

-- ERROR: Error: 1049 (42000): Unknown database 'hosbill_bpg' 


