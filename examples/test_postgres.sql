-- url: postgresql://myuser:mypassword@localhost:5432/mydb

-- Create a test table
CREATE TABLE IF NOT EXISTS test_sqlazo (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
