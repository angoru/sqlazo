-- url: mysql://myuser:mypassword@localhost:3306/mydb

-- Example: Destructive query that will trigger safety confirmation
-- When you run this with :SqlazoConsole or :SqlazoRun, 
-- you'll see: "⚠️ Query contains UPDATE. Execute anyway?"

UPDATE users 
SET name = 'test_name' 
WHERE id = 999999;  -- Non-existent ID for safety

-- Another example: DELETE query
-- DELETE FROM users WHERE id = 999999;

-- To disable confirmation, add this to your Neovim config:
-- require("sqlazo").setup({ safe_mode = false })
