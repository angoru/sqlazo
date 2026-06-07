-- Entry point for the Neovim Lua test suite.
-- Run with: nvim --headless -u NONE -l tests/nvim/run.lua

local cwd = vim.fn.getcwd()
vim.opt.runtimepath:append(cwd .. "/nvim")
package.path = cwd .. "/tests/nvim/?.lua;" .. package.path

local t = require("harness")

print("results_spec")
require("results_spec")
print("schema_spec")
require("schema_spec")

t.finish()
