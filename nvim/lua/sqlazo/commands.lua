-- sqlazo.nvim commands

local M = {}

local config = require("sqlazo.config")
local runner = require("sqlazo.runner")

function M.setup()
  vim.api.nvim_create_user_command("SqlazoRun", function()
    runner.run({
      format = config.get().format,
    })
  end, { desc = "Execute query at cursor" })

  vim.api.nvim_create_user_command("SqlazoFilterValue", function()
    runner.filter_by_selected_value()
  end, { desc = "Filter source query by selected result cell" })
end

return M
