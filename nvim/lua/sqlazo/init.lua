-- sqlazo.nvim

local M = {}

M.version = "0.1.1"

local config = require("sqlazo.config")
local commands = require("sqlazo.commands")
local runner = require("sqlazo.runner")
local schema = require("sqlazo.schema")

function M.setup(opts)
  config.setup(opts)
  runner.reset_detection_cache()
  commands.setup()
end

M.run = runner.run

function M.setup_cmp()
  return schema.setup_cmp()
end

return M
