-- sqlazo.nvim configuration module

local M = {}

-- Default configuration
M.defaults = {
  format = "table",      -- Output format: table, csv, json
  split = "float",       -- Split direction: horizontal, vertical, float
  python_cmd = "python", -- Python command (python, python3, etc.)
  safe_mode = true,      -- Confirm before executing destructive queries
}

-- Current configuration (merged with defaults)
M.config = vim.tbl_deep_extend("force", {}, M.defaults)

-- Destructive SQL keywords that modify database
M.destructive_keywords = {
  "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
  "CREATE", "REPLACE", "RENAME", "GRANT", "REVOKE"
}

-- Setup configuration
function M.setup(opts)
  M.config = vim.tbl_deep_extend("force", M.defaults, opts or {})
end

-- Get current config
function M.get()
  return M.config
end

-- Check if query contains destructive operations
function M.is_destructive_query(query)
  local upper_query = query:upper()
  for _, keyword in ipairs(M.destructive_keywords) do
    if upper_query:match("^%s*" .. keyword .. "%s") or
       upper_query:match("\n%s*" .. keyword .. "%s") then
      return true, keyword
    end
  end
  return false, nil
end

-- Confirm destructive query execution
function M.confirm_destructive(keyword, callback)
  vim.ui.select(
    {"No, cancel", "Yes, execute"},
    {
      prompt = "⚠️  Query contains " .. keyword .. ". Execute anyway?",
    },
    function(choice)
      if choice == "Yes, execute" then
        callback()
      else
        vim.api.nvim_echo({{"sqlazo: Query cancelled", "WarningMsg"}}, true, {})
      end
    end
  )
end

return M
