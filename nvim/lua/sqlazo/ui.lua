-- sqlazo.nvim UI module
-- Handles floating windows and splits

local M = {}

-- Open buffer in floating window
function M.open_float(buf, opts)
  opts = opts or {}
  local width = opts.width or math.floor(vim.o.columns * 0.8)
  local height = opts.height or math.floor(vim.o.lines * 0.8)
  local row = opts.row or math.floor((vim.o.lines - height) / 2)
  local col = opts.col or math.floor((vim.o.columns - width) / 2)

  local win_opts = {
    relative = "editor",
    width = width,
    height = height,
    row = row,
    col = col,
    style = "minimal",
    border = "rounded",
    title = opts.title or " sqlazo results ",
    title_pos = "center",
  }

  local win = vim.api.nvim_open_win(buf, true, win_opts)

  -- Close on q or Escape
  vim.api.nvim_buf_set_keymap(buf, "n", "q", ":close<CR>", { noremap = true, silent = true })
  vim.api.nvim_buf_set_keymap(buf, "n", "<Esc>", ":close<CR>", { noremap = true, silent = true })

  return win
end

-- Create a scratch buffer
function M.create_scratch_buffer(opts)
  opts = opts or {}
  local buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_buf_set_option(buf, "buftype", "nofile")
  vim.api.nvim_buf_set_option(buf, "bufhidden", "wipe")
  vim.api.nvim_buf_set_option(buf, "swapfile", false)

  if opts.filetype then
    vim.api.nvim_buf_set_option(buf, "filetype", opts.filetype)
  end

  if opts.modifiable == false then
    vim.api.nvim_buf_set_option(buf, "modifiable", false)
  end

  if opts.name then
    pcall(vim.api.nvim_buf_set_name, buf, opts.name)
  end

  return buf
end

-- Open buffer in split
function M.open_split(buf, direction)
  if direction == "vertical" then
    vim.cmd("vsplit")
  else
    vim.cmd("split")
  end
  vim.api.nvim_win_set_buf(0, buf)
end

-- Calculate console dimensions
function M.get_console_dimensions()
  local width = math.floor(vim.o.columns * 0.8)
  local height = math.floor(vim.o.lines * 0.8)
  local row = math.floor((vim.o.lines - height) / 2)
  local col = math.floor((vim.o.columns - width) / 2)
  local query_height = math.floor(height * 0.4)
  local result_height = height - query_height - 1

  return {
    width = width,
    height = height,
    row = row,
    col = col,
    query_height = query_height,
    result_height = result_height,
  }
end

return M
