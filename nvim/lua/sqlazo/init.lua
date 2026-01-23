-- sqlazo.nvim - Execute SQL queries from Neovim
-- Lua plugin for sqlazo CLI tool

local M = {}

M.version = "0.1.1"

local config = require("sqlazo.config")
local commands = require("sqlazo.commands")
local runner = require("sqlazo.runner")
local console = require("sqlazo.console")
local schema = require("sqlazo.schema")
local parser = require("sqlazo.parser")

-- Setup function to configure the plugin
function M.setup(opts)
  config.setup(opts)
  commands.setup()
end

-- Re-export main functions for convenience
M.run = runner.run
M.run_inline = runner.run_inline
M.run_all_inline = runner.run_all_inline
M.open_console = console.open
M.get_schema = schema.get
M.clear_schema_cache = schema.clear_cache
M.setup_cmp = schema.setup_cmp

-- Re-export config access
M.config = config.config

return M
