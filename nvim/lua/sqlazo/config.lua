-- sqlazo.nvim configuration

local M = {}

M.defaults = {
  sqlazo_cmd = "sqlazo",
  safe_mode = true,
  default_comment_prefix = "--",
  comment_prefix_by_filetype = {
    sql = "--",
    mysql = "--",
    pgsql = "--",
    psql = "--",
    plsql = "--",
    sqlite = "--",
    javascript = "//",
  },
}

M.config = vim.tbl_deep_extend("force", {}, M.defaults)

M.destructive_keywords = {
  "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
  "CREATE", "REPLACE", "RENAME", "GRANT", "REVOKE",
}

function M.setup(opts)
  M.config = vim.tbl_deep_extend("force", M.defaults, opts or {})
end

function M.get()
  return M.config
end

function M.is_destructive_query(query)
  local upper_query = query:upper()
  for _, keyword in ipairs(M.destructive_keywords) do
    if upper_query:match("^%s*" .. keyword .. "%s") or upper_query:match("\n%s*" .. keyword .. "%s") then
      return true, keyword
    end
  end
  return false, nil
end

function M.confirm_destructive(keyword, callback)
  vim.ui.select({ "No, cancel", "Yes, execute" }, {
    prompt = "Query contains " .. keyword .. ". Execute anyway?",
  }, function(choice)
    if choice == "Yes, execute" then
      callback()
    else
      vim.api.nvim_echo({ { "sqlazo: Query cancelled", "WarningMsg" } }, true, {})
    end
  end)
end

return M
