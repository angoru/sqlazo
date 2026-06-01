-- dbtype: mysql
-- host: db.example.local
-- port: 3306
-- user: app_user
-- database: app_db

-- Example: Destructive query that will trigger safety confirmation
-- When you run this with :SqlazoRun, sqlazo.nvim asks for confirmation.

UPDATE users 
SET name = 'test_name' 
WHERE id = 999999;  -- Non-existent ID for safety

-- Another example: DELETE query
-- DELETE FROM users WHERE id = 999999;

-- To disable confirmation, add this to your Neovim config:
-- require("sqlazo").setup({ safe_mode = false })
