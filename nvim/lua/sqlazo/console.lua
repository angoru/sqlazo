-- sqlazo.nvim console module
-- Interactive SQL console with split windows

local M = {}

local config = require("sqlazo.config")
local parser = require("sqlazo.parser")
local results = require("sqlazo.results")
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
  local initial_query_line = #header_lines > 0 and (#header_lines + 2) or 1

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
    title = " SQL Query (F5/Enter: exec, Ctrl+S: save, Tab: results, g?: help, q: close) ",
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
    title = " Results (g?: help, q: close) ",
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
  local function focus_query()
    if query_win and vim.api.nvim_win_is_valid(query_win) then
      vim.api.nvim_set_current_win(query_win)
    end
  end

  local function focus_results()
    if result_win and vim.api.nvim_win_is_valid(result_win) then
      vim.api.nvim_set_current_win(result_win)
    end
  end

  local function execute_query()
    local lines = vim.api.nvim_buf_get_lines(query_buf, 0, -1, false)
    local content = table.concat(lines, "\n")

    local function do_execute()
      local result, error_message, exit_code = runner.execute_meta(content, config.get().profile)
      if exit_code ~= 0 then
        results.set_buffer_error(result_buf, error_message)
        vim.api.nvim_echo({{"sqlazo: Query failed", "ErrorMsg"}}, true, {})
      else
        results.set_buffer_result(result_buf, result)
        vim.api.nvim_echo({{"sqlazo: Query executed", "Normal"}}, true, {})
        focus_results()
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

  local function show_console_help()
    ui.show_help("sqlazo console", {
      "Query",
      "  <F5> / <C-x>    execute query",
      "  <CR>            execute query in normal mode",
      "  <C-s>           save query back to source buffer",
      "  <Tab> / <C-j>   focus results",
      "",
      "Results",
      "  h / j / k / l   move selected cell",
      "  yc              copy selected cell",
      "  yr              copy selected row as CSV",
      "  yC              copy selected column as CSV",
      "  e               export result to CSV",
      "  r               re-run query",
      "  gq / <BS>       focus query",
      "",
      "Window",
      "  q               close console",
      "  g?              toggle this help",
    })
  end

  -- Keymaps for query buffer
  vim.keymap.set({"n", "i"}, "<C-x>", execute_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set({"n", "i"}, "<F5>", execute_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set("n", "<leader>r", execute_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set("n", "<CR>", execute_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set("n", "q", close_console, { buffer = query_buf, desc = "Close console" })
  vim.keymap.set("n", "g?", show_console_help, { buffer = query_buf, desc = "Show console help" })
  vim.keymap.set({"n", "i"}, "<C-s>", save_to_source, { buffer = query_buf, desc = "Save query" })

  -- Keymaps for result buffer
  vim.keymap.set("n", "q", close_console, { buffer = result_buf, desc = "Close console" })
  vim.keymap.set("n", "g?", show_console_help, { buffer = result_buf, desc = "Show console help" })
  vim.keymap.set("n", "<Tab>", focus_query, { buffer = result_buf, desc = "Switch to query" })
  vim.keymap.set("n", "<BS>", focus_query, { buffer = result_buf, desc = "Switch to query" })
  vim.keymap.set("n", "gq", focus_query, { buffer = result_buf, desc = "Switch to query" })
  vim.keymap.set("n", "r", execute_query, { buffer = result_buf, desc = "Re-run query" })
  vim.keymap.set("n", "h", function() results.move_selection(result_buf, 0, -1) end, { buffer = result_buf, desc = "Move cell left" })
  vim.keymap.set("n", "j", function() results.move_selection(result_buf, 1, 0) end, { buffer = result_buf, desc = "Move cell down" })
  vim.keymap.set("n", "k", function() results.move_selection(result_buf, -1, 0) end, { buffer = result_buf, desc = "Move cell up" })
  vim.keymap.set("n", "l", function() results.move_selection(result_buf, 0, 1) end, { buffer = result_buf, desc = "Move cell right" })
  vim.keymap.set("n", "yc", function() results.copy_cell(result_buf) end, { buffer = result_buf, desc = "Copy cell" })
  vim.keymap.set("n", "yr", function() results.copy_row(result_buf) end, { buffer = result_buf, desc = "Copy row" })
  vim.keymap.set("n", "yC", function() results.copy_column(result_buf) end, { buffer = result_buf, desc = "Copy column" })
  vim.keymap.set("n", "e", function() results.export_csv(result_buf) end, { buffer = result_buf, desc = "Export CSV" })

  vim.keymap.set("n", "<Tab>", focus_results, { buffer = query_buf, desc = "Switch to results" })
  vim.keymap.set({"n", "i"}, "<C-j>", focus_results, { buffer = query_buf, desc = "Switch to results" })
  vim.keymap.set("n", "<C-k>", focus_query, { buffer = result_buf, desc = "Switch to query" })

  vim.api.nvim_set_current_win(query_win)
  pcall(vim.api.nvim_win_set_cursor, query_win, { initial_query_line, 0 })
end

return M
