-- sqlazo.nvim console module
-- Interactive SQL console with split windows

local M = {}

local config = require("sqlazo.config")
local parser = require("sqlazo.parser")
local runner = require("sqlazo.runner")
local ui = require("sqlazo.ui")

-- Console state
M.state = {
  query_buf = nil,
  result_buf = nil,
  query_win = nil,
  result_win = nil,
  source_buf = nil,
  source_query_start = nil,
  source_query_end = nil,
  source_header_end = nil,
}

-- Open interactive SQL console
function M.open()
  local dims = ui.get_console_dimensions()

  -- Create query buffer (editable)
  local query_buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_buf_set_option(query_buf, "buftype", "nofile")
  vim.api.nvim_buf_set_option(query_buf, "bufhidden", "wipe")
  vim.api.nvim_buf_set_option(query_buf, "filetype", "sql")

  -- Store source buffer
  local source_buf = vim.api.nvim_get_current_buf()

  -- Get header and query from current buffer
  local current_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = parser.get_header(current_lines)
  local query_lines, query_start, query_end = parser.get_query_at_cursor(current_lines)

  -- Build initial content
  local initial_lines = {}
  for _, line in ipairs(header_lines) do
    table.insert(initial_lines, line)
  end
  if #header_lines > 0 then
    table.insert(initial_lines, "")
  end
  for _, line in ipairs(query_lines) do
    table.insert(initial_lines, line)
  end

  if #initial_lines > 0 then
    vim.api.nvim_buf_set_lines(query_buf, 0, -1, false, initial_lines)
  end

  -- Create result buffer (readonly)
  local result_buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_buf_set_option(result_buf, "buftype", "nofile")
  vim.api.nvim_buf_set_option(result_buf, "bufhidden", "wipe")
  vim.api.nvim_buf_set_option(result_buf, "modifiable", false)

  -- Open query window (top)
  local query_win = vim.api.nvim_open_win(query_buf, true, {
    relative = "editor",
    width = dims.width,
    height = dims.query_height,
    row = dims.row,
    col = dims.col,
    style = "minimal",
    border = "rounded",
    title = " SQL Query (F5/Enter: exec, Ctrl+S: save, q: close) ",
    title_pos = "center",
  })

  -- Open result window (bottom)
  local result_win = vim.api.nvim_open_win(result_buf, false, {
    relative = "editor",
    width = dims.width,
    height = dims.result_height,
    row = dims.row + dims.query_height + 2,
    col = dims.col,
    style = "minimal",
    border = "rounded",
    title = " Results ",
    title_pos = "center",
  })

  -- Store state
  M.state = {
    query_buf = query_buf,
    result_buf = result_buf,
    query_win = query_win,
    result_win = result_win,
    source_buf = source_buf,
    source_query_start = query_start,
    source_query_end = query_end,
    source_header_end = #header_lines,
  }

  -- Define local functions
  local function execute_query()
    local lines = vim.api.nvim_buf_get_lines(query_buf, 0, -1, false)
    local content = table.concat(lines, "\n")

    local function do_execute()
      local output, exit_code = runner.execute(content, config.get().format)

      vim.api.nvim_buf_set_option(result_buf, "modifiable", true)
      local result_lines = vim.split(output, "\n", { trimempty = false })
      vim.api.nvim_buf_set_lines(result_buf, 0, -1, false, result_lines)
      vim.api.nvim_buf_set_option(result_buf, "modifiable", false)

      if exit_code ~= 0 then
        vim.api.nvim_echo({{"sqlazo: Query failed", "ErrorMsg"}}, true, {})
      else
        vim.api.nvim_echo({{"sqlazo: Query executed", "Normal"}}, true, {})
      end
    end

    if config.get().safe_mode then
      local is_destructive, keyword = config.is_destructive_query(content)
      if is_destructive then
        config.confirm_destructive(keyword, do_execute)
        return
      end
    end

    do_execute()
  end

  local function close_console()
    pcall(vim.api.nvim_win_close, result_win, true)
    pcall(vim.api.nvim_win_close, query_win, true)
    M.state = {}
  end

  local function save_to_source()
    if not vim.api.nvim_buf_is_valid(source_buf) then
      vim.api.nvim_echo({{"sqlazo: Source buffer no longer exists", "ErrorMsg"}}, true, {})
      return
    end

    local console_lines = vim.api.nvim_buf_get_lines(query_buf, 0, -1, false)
    local console_header = parser.get_header(console_lines)

    local query_only = {}
    local skip_count = #console_header
    if #console_header > 0 then
      skip_count = skip_count + 1
    end
    for i = skip_count + 1, #console_lines do
      table.insert(query_only, console_lines[i])
    end

    vim.api.nvim_buf_set_lines(source_buf, query_start - 1, query_end, false, query_only)
    M.state.source_query_end = query_start - 1 + #query_only

    local source_name = vim.api.nvim_buf_get_name(source_buf)
    if source_name == "" then
      source_name = "[Scratch]"
    else
      source_name = vim.fn.fnamemodify(source_name, ":t")
    end

    vim.api.nvim_echo({{"sqlazo: Query saved to " .. source_name, "Normal"}}, true, {})
  end

  -- Keymaps for query buffer
  vim.keymap.set({"n", "i"}, "<C-x>", execute_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set({"n", "i"}, "<F5>", execute_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set("n", "<leader>r", execute_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set("n", "<CR>", execute_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set("n", "q", close_console, { buffer = query_buf, desc = "Close console" })
  vim.keymap.set("n", "<Esc>", close_console, { buffer = query_buf, desc = "Close console" })
  vim.keymap.set({"n", "i"}, "<C-s>", save_to_source, { buffer = query_buf, desc = "Save query" })

  -- Keymaps for result buffer
  vim.keymap.set("n", "q", close_console, { buffer = result_buf, desc = "Close console" })
  vim.keymap.set("n", "<Esc>", close_console, { buffer = result_buf, desc = "Close console" })
  vim.keymap.set("n", "<Tab>", function()
    vim.api.nvim_set_current_win(query_win)
  end, { buffer = result_buf, desc = "Switch to query" })

  vim.keymap.set("n", "<Tab>", function()
    vim.api.nvim_set_current_win(result_win)
  end, { buffer = query_buf, desc = "Switch to results" })

  vim.cmd("startinsert!")
end

return M
