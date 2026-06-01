-- Auto-load sqlazo.nvim commands when the plugin is on runtimepath.

if vim.g.loaded_sqlazo_nvim == 1 then
  return
end
vim.g.loaded_sqlazo_nvim = 1

local ok, sqlazo = pcall(require, "sqlazo")
if ok then
  sqlazo.setup(vim.g.sqlazo_config or {})
end
