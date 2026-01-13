-- sqlazo.nvim commands module
-- Vim command definitions

local M = {}

local runner = require("sqlazo.runner")
local console = require("sqlazo.console")

-- Register all user commands
function M.setup()
  vim.api.nvim_create_user_command("SqlazoRun", function()
    runner.run()
  end, { desc = "Execute current query and show results" })

  vim.api.nvim_create_user_command("SqlazoRunAll", function()
    runner.run({ query_mode = "all" })
  end, { desc = "Execute all queries in buffer" })

  vim.api.nvim_create_user_command("SqlazoRunVertical", function()
    runner.run({ split = "vertical" })
  end, { desc = "Execute current query in vertical split" })

  vim.api.nvim_create_user_command("SqlazoRunHorizontal", function()
    runner.run({ split = "horizontal" })
  end, { desc = "Execute current query in horizontal split" })

  vim.api.nvim_create_user_command("SqlazoRunFloat", function()
    runner.run({ split = "float" })
  end, { desc = "Execute current query in floating window" })

  vim.api.nvim_create_user_command("SqlazoRunRecord", function()
    runner.run({ format = "record" })
  end, { desc = "Execute current query with record format" })

  vim.api.nvim_create_user_command("SqlazoRunInline", function(opts)
    local max_rows = 5
    if opts.args and opts.args ~= "" then
      max_rows = tonumber(opts.args) or 5
    end
    runner.run_inline(max_rows)
  end, { nargs = "?", desc = "Insert first N rows below query as comments" })

  vim.api.nvim_create_user_command("SqlazoRunAllInline", function(opts)
    local max_rows = 5
    if opts.args and opts.args ~= "" then
      max_rows = tonumber(opts.args) or 5
    end
    runner.run_all_inline(max_rows)
  end, { nargs = "?", desc = "Run all queries and update inline results" })

  vim.api.nvim_create_user_command("SqlazoConsole", function()
    console.open()
  end, { desc = "Open interactive SQL console" })
end

return M
