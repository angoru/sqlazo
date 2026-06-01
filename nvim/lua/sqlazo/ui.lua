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

function M.show_help(title, lines)
  local width = 0
  for _, line in ipairs(lines) do
    width = math.max(width, #line)
  end
  width = math.min(math.max(width + 4, 36), math.floor(vim.o.columns * 0.8))
  local height = math.min(#lines, math.floor(vim.o.lines * 0.6))

  local buf = M.create_scratch_buffer({
    filetype = "sqlazo-help",
    modifiable = true,
  })
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
  vim.api.nvim_buf_set_option(buf, "modifiable", false)

  local win = vim.api.nvim_open_win(buf, true, {
    relative = "editor",
    width = width,
    height = height,
    row = math.floor((vim.o.lines - height) / 2),
    col = math.floor((vim.o.columns - width) / 2),
    style = "minimal",
    border = "rounded",
    title = " " .. title .. " ",
    title_pos = "center",
  })

  vim.keymap.set("n", "q", function() pcall(vim.api.nvim_win_close, win, true) end, { buffer = buf, silent = true })
  vim.keymap.set("n", "<Esc>", function() pcall(vim.api.nvim_win_close, win, true) end, { buffer = buf, silent = true })
  vim.keymap.set("n", "g?", function() pcall(vim.api.nvim_win_close, win, true) end, { buffer = buf, silent = true })

  return win
end

-- Create a scratch buffer
function M.create_scratch_buffer(opts)
  opts = opts or {}
  local buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_buf_set_option(buf, "buftype", "nofile")
  vim.api.nvim_buf_set_option(buf, "bufhidden", opts.bufhidden or "wipe")
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

function M.open_tab(buf)
  vim.cmd("tabnew")
  vim.api.nvim_win_set_buf(0, buf)
  return vim.api.nvim_get_current_win()
end

function M.open_panel(buf, opts)
  opts = opts or {}
  local position = opts.position or "bottom"

  if position == "right" then
    vim.cmd("botright vertical " .. tostring(opts.width or 80) .. "split")
  else
    vim.cmd("botright " .. tostring(opts.height or 12) .. "split")
  end

  vim.api.nvim_win_set_buf(0, buf)
  return vim.api.nvim_get_current_win()
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
