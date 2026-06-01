-- dbtype: postgresql
-- host: db.example.local
-- port: 5432
-- user: app_user
-- database: app_db

-- Create a test table
CREATE TABLE IF NOT EXISTS test_sqlazo (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
