-- sqlazo.nvim runner module
-- Handles query execution via CLI

local M = {}

local config = require("sqlazo.config")
local parser = require("sqlazo.parser")
local results = require("sqlazo.results")
local ui = require("sqlazo.ui")

M.last_results = {}

local function python_cmd()
  local cmd = vim.split(config.get().python_cmd, "%s+", { trimempty = true })
  table.insert(cmd, "-m")
  table.insert(cmd, "sqlazo")
  return cmd
end

local function cmd_with_args(cmd, args)
  local copy = vim.deepcopy(cmd)
  for _, arg in ipairs(args) do
    table.insert(copy, arg)
  end
  return copy
end

local function command_supports_json_meta(cmd)
  local help = vim.fn.system(cmd_with_args(cmd, { "query", "--help" }))
  return vim.v.shell_error == 0 and help:match("json%-meta") ~= nil
end

-- Get the sqlazo command
function M.get_cmd()
  local cfg = config.get()
  local py_cmd = python_cmd()

  if cfg.prefer_python then
    return py_cmd
  end

  if vim.fn.executable("sqlazo") == 1 then
    local path_cmd = { "sqlazo" }
    if cfg.auto_prefer_json_meta and not command_supports_json_meta(path_cmd) and command_supports_json_meta(py_cmd) then
      return py_cmd
    end
    return path_cmd
  end

  return py_cmd
end

local supports_query_subcommand
local supports_json_meta

function M.reset_detection_cache()
  supports_query_subcommand = nil
  supports_json_meta = nil
end

function M.uses_query_subcommand()
  if supports_query_subcommand ~= nil then
    return supports_query_subcommand
  end

  local cmd = M.get_cmd()
  table.insert(cmd, "--help")
  local help = vim.fn.system(cmd)
  supports_query_subcommand = vim.v.shell_error == 0 and help:match("query") ~= nil
  return supports_query_subcommand
end

function M.supports_json_meta()
  if supports_json_meta ~= nil then
    return supports_json_meta
  end

  supports_json_meta = command_supports_json_meta(M.get_cmd())
  return supports_json_meta
end

local function build_query_cmd(format, profile)
  local cmd = M.get_cmd()
  if M.uses_query_subcommand() then
    table.insert(cmd, "query")
  end
  table.insert(cmd, "-f")
  table.insert(cmd, format or config.get().format)
  if profile then
    table.insert(cmd, "--profile")
    table.insert(cmd, profile)
  end
  table.insert(cmd, "-")
  return cmd
end

-- Execute a query and return output
function M.execute(content, format, profile)
  format = format or config.get().format
  local cmd = build_query_cmd(format, profile)
  local output = vim.fn.system(cmd, content)
  local exit_code = vim.v.shell_error
  return output, exit_code
end

-- Execute a query using stored credentials
function M.execute_with_profile(content, format, profile)
  return M.execute(content, format, profile)
end

function M.execute_meta(content, profile)
  if not M.supports_json_meta() then
    local output, exit_code = M.execute(content, "table", profile)
    if exit_code ~= 0 then
      return nil, output, exit_code
    end

    return {
      raw_output = output,
      metadata = {
        mode = "legacy table",
      },
    }, nil, 0
  end

  local output, exit_code = M.execute(content, "json-meta", profile)
  if exit_code ~= 0 then
    return nil, output, exit_code
  end

  local ok, decoded = pcall(vim.json.decode, output)
  if not ok then
    return nil, "Failed to parse sqlazo json-meta output: " .. tostring(decoded), 1
  end

  return decoded, nil, 0
end

local function jump_to_source(buf)
  local source_buf = vim.b[buf].sqlazo_source_buf
  local source_win = vim.b[buf].sqlazo_source_win
  local query_start = vim.b[buf].sqlazo_query_start

  if source_win and vim.api.nvim_win_is_valid(source_win) then
    vim.api.nvim_set_current_win(source_win)
  elseif source_buf and vim.api.nvim_buf_is_valid(source_buf) then
    vim.api.nvim_set_current_buf(source_buf)
  else
    vim.api.nvim_echo({{"sqlazo: Source buffer no longer exists", "ErrorMsg"}}, true, {})
    return
  end

  if query_start then
    pcall(vim.api.nvim_win_set_cursor, 0, { query_start, 0 })
  end
end

local function setup_result_keymaps(buf, rerun)
  local function show_result_help()
    ui.show_help("sqlazo results", {
      "Navigation",
      "  h / j / k / l   move selected cell",
      "  gq / <BS>       jump back to query",
      "  r               re-run query",
      "",
      "Copy / Export",
      "  yc              copy selected cell",
      "  yr              copy selected row as CSV",
      "  yC              copy selected column as CSV",
      "  e               export result to CSV",
      "",
      "Window",
      "  q / <Esc>       close result",
      "  g?              toggle this help",
    })
  end

  vim.keymap.set("n", "q", ":close<CR>", { buffer = buf, noremap = true, silent = true, desc = "Close results" })
  vim.keymap.set("n", "<Esc>", ":close<CR>", { buffer = buf, noremap = true, silent = true, desc = "Close results" })
  vim.keymap.set("n", "g?", show_result_help, { buffer = buf, desc = "Show result help" })
  vim.keymap.set("n", "gq", function() jump_to_source(buf) end, { buffer = buf, desc = "Jump to query" })
  vim.keymap.set("n", "<BS>", function() jump_to_source(buf) end, { buffer = buf, desc = "Jump to query" })
  vim.keymap.set("n", "r", rerun, { buffer = buf, desc = "Re-run query" })
  vim.keymap.set("n", "h", function() results.move_selection(buf, 0, -1) end, { buffer = buf, desc = "Move cell left" })
  vim.keymap.set("n", "j", function() results.move_selection(buf, 1, 0) end, { buffer = buf, desc = "Move cell down" })
  vim.keymap.set("n", "k", function() results.move_selection(buf, -1, 0) end, { buffer = buf, desc = "Move cell up" })
  vim.keymap.set("n", "l", function() results.move_selection(buf, 0, 1) end, { buffer = buf, desc = "Move cell right" })
  vim.keymap.set("n", "yc", function() results.copy_cell(buf) end, { buffer = buf, desc = "Copy cell" })
  vim.keymap.set("n", "yr", function() results.copy_row(buf) end, { buffer = buf, desc = "Copy row" })
  vim.keymap.set("n", "yC", function() results.copy_column(buf) end, { buffer = buf, desc = "Copy column" })
  vim.keymap.set("n", "e", function() results.export_csv(buf) end, { buffer = buf, desc = "Export CSV" })
end

function M.focus_last_result()
  local source_buf = vim.api.nvim_get_current_buf()
  local state = M.last_results[source_buf]

  if not state or not state.buf or not vim.api.nvim_buf_is_valid(state.buf) then
    vim.api.nvim_echo({{"sqlazo: No result buffer for current query", "WarningMsg"}}, true, {})
    return
  end

  if state.win and vim.api.nvim_win_is_valid(state.win) then
    vim.api.nvim_set_current_win(state.win)
    return
  end

  if state.mode == "tab" then
    state.win = ui.open_tab(state.buf)
  elseif state.mode == "float" then
    state.win = ui.open_float(state.buf)
  elseif state.mode == "panel" then
    state.win = ui.open_panel(state.buf, {
      position = state.position,
      height = state.height,
      width = state.width,
    })
  else
    ui.open_split(state.buf, state.split or "horizontal")
    state.win = vim.api.nvim_get_current_win()
  end
end

local function result_mode(opts)
  if opts.result_mode then
    return opts.result_mode
  end
  if opts.split == "float" then
    return "float"
  end
  if opts.split == "vertical" or opts.split == "horizontal" then
    return "split"
  end
  return "panel"
end

local function result_buffer_name(source_name, mode)
  if mode == "panel" then
    return "sqlazo://" .. source_name .. "/results"
  end
  return "sqlazo://" .. source_name .. "." .. os.date("%H%M%S")
end

local function get_or_create_result_buffer(source_buf, source_name, mode, opts)
  local state = M.last_results[source_buf]
  if mode == "panel" and opts.reuse_result_buffer and state and state.buf and vim.api.nvim_buf_is_valid(state.buf) then
    return state.buf
  end

  return ui.create_scratch_buffer({
    name = result_buffer_name(source_name, mode),
    filetype = "sqlazo-result",
    bufhidden = mode == "panel" and "hide" or "wipe",
  })
end

local function open_result_buffer(buf, mode, opts)
  if mode == "tab" then
    return ui.open_tab(buf)
  end
  if mode == "float" then
    return ui.open_float(buf)
  end
  if mode == "panel" then
    local state = M.last_results[vim.b[buf].sqlazo_source_buf]
    if state and state.win and vim.api.nvim_win_is_valid(state.win) then
      vim.api.nvim_set_current_win(state.win)
      vim.api.nvim_win_set_buf(state.win, buf)
      return state.win
    end
    return ui.open_panel(buf, {
      position = opts.result_position,
      height = opts.result_height,
      width = opts.result_width,
    })
  end

  ui.open_split(buf, opts.split or "horizontal")
  return vim.api.nvim_get_current_win()
end

-- Execute current query and show in window
function M.run(opts)
  opts = vim.tbl_deep_extend("force", {}, config.get(), opts or {})
  opts.query_mode = opts.query_mode or "cursor"

  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = parser.get_header(all_lines)

  local query_lines
  if opts.query_mode == "all" then
    query_lines = {}
    local in_header = true
    for _, line in ipairs(all_lines) do
      local trimmed = line:match("^%s*(.-)%s*$")
      if in_header then
        if not parser.is_header_line(line) and trimmed ~= "" then
          in_header = false
          table.insert(query_lines, line)
        end
      else
        table.insert(query_lines, line)
      end
    end
  else
    query_lines = parser.get_query_at_cursor(all_lines)
  end

  if #query_lines == 0 then
    vim.api.nvim_echo({{"sqlazo: No query found at cursor", "WarningMsg"}}, true, {})
    return
  end

  local content = parser.build_content(header_lines, query_lines)

  local function do_run()
    local source_name = vim.fn.expand("%:t")
    if source_name == "" then source_name = "[Scratch]" end
    local source_buf = vim.api.nvim_get_current_buf()
    local mode = result_mode(opts)

    local buf = get_or_create_result_buffer(source_buf, source_name, mode, opts)

    vim.b[buf].sqlazo_source_buf = source_buf
    vim.b[buf].sqlazo_source_win = vim.api.nvim_get_current_win()
    vim.b[buf].sqlazo_query_start = select(2, parser.get_query_at_cursor(all_lines))

    local function render()
      local result, error_message, exit_code = M.execute_meta(content, opts.profile)
      if exit_code ~= 0 then
        results.set_buffer_error(buf, error_message)
        vim.api.nvim_echo({{"sqlazo: Query failed (see output)", "ErrorMsg"}}, true, {})
      else
        results.set_buffer_result(buf, result)
        vim.api.nvim_echo({{"sqlazo: Query executed", "Normal"}}, true, {})
      end
    end

    render()
    setup_result_keymaps(buf, render)

    local result_win = open_result_buffer(buf, mode, opts)

    M.last_results[source_buf] = {
      buf = buf,
      win = result_win,
      mode = mode,
      split = opts.split,
      position = opts.result_position,
      height = opts.result_height,
      width = opts.result_width,
    }
  end

  if config.get().safe_mode then
    local is_destructive, keyword = config.is_destructive_query(content)
    if is_destructive then
      config.confirm_destructive(keyword, do_run)
      return
    end
  end

  do_run()
end

-- Execute query and insert results inline as comments
function M.run_inline(max_rows, opts)
  opts = opts or {}
  max_rows = max_rows or 5

  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = parser.get_header(all_lines)
  local query_lines, query_start, query_end = parser.get_query_at_cursor(all_lines)
  local content = parser.build_content(header_lines, query_lines)
  local comment_prefix = parser.get_comment_prefix(all_lines)

  local output, exit_code
  if opts.profile then
    output, exit_code = M.execute_with_profile(content, "table", opts.profile)
  else
    output, exit_code = M.execute(content, "table")
  end

  if exit_code ~= 0 then
    vim.api.nvim_echo({{"sqlazo: Query failed - " .. output, "ErrorMsg"}}, true, {})
    return
  end

  local result_lines = vim.split(output, "\n", { trimempty = false })
  local inline_lines = {}
  local row_count = 0
  local header_found = false

  for i, line in ipairs(result_lines) do
    if line:match("^%s*$") and #inline_lines == 0 then
      goto continue
    end

    if line:match("^%(%d+ rows?%)$") or line:match("^Affected rows:") then
      table.insert(inline_lines, comment_prefix .. " " .. line)
      break
    end

    if line:match("^|.*|$") and not line:match("^%+%-") then
      if header_found then
        row_count = row_count + 1
      else
        header_found = true
      end
    end

    table.insert(inline_lines, comment_prefix .. " " .. line)

    if row_count >= max_rows then
      if result_lines[i + 1] and result_lines[i + 1]:match("^%+%-") then
        table.insert(inline_lines, comment_prefix .. " " .. result_lines[i + 1])
      end
      for _, l in ipairs(result_lines) do
        if l:match("^%(%d+ rows?%)$") then
          table.insert(inline_lines, comment_prefix .. " " .. l)
          break
        end
      end
      break
    end

    ::continue::
  end

  table.insert(inline_lines, 1, "")
  vim.api.nvim_buf_set_lines(0, query_end, query_end, false, inline_lines)
  vim.api.nvim_echo({{"sqlazo: Inserted " .. row_count .. " rows inline", "Normal"}}, true, {})
end

-- Run all queries and update inline results
function M.run_all_inline(max_rows, opts)
  opts = opts or {}
  max_rows = max_rows or 5

  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = parser.get_header(all_lines)
  local header_content = table.concat(header_lines, "\n")
  local comment_prefix = parser.get_comment_prefix(all_lines)

  -- Remove existing inline results
  local cleaned_lines = {}
  for _, line in ipairs(all_lines) do
    if not parser.is_inline_result(line, comment_prefix) then
      table.insert(cleaned_lines, line)
    end
  end

  local queries = parser.find_all_queries(cleaned_lines)

  if #queries == 0 then
    vim.api.nvim_echo({{"sqlazo: No queries found", "WarningMsg"}}, true, {})
    return
  end

  local results = {}
  for _, q in ipairs(queries) do
    local query_text = {}
    for i = q.start, q.finish do
      table.insert(query_text, cleaned_lines[i])
    end

    local content = header_content .. "\n\n" .. table.concat(query_text, "\n")
    local output, exit_code
    if opts.profile then
      output, exit_code = M.execute_with_profile(content, "table", opts.profile)
    else
      output, exit_code = M.execute(content, "table")
    end

    if exit_code ~= 0 then
      table.insert(results, { pos = q.finish, lines = {"", comment_prefix .. " ERROR: " .. output:gsub("\n", " ")} })
    else
      local result_lines = vim.split(output, "\n", { trimempty = false })
      local inline_lines = {}
      local row_count = 0
      local header_found = false

      for i, line in ipairs(result_lines) do
        if line:match("^%s*$") and #inline_lines == 0 then
          goto skip
        end

        if line:match("^%(%d+ rows?%)$") or line:match("^Affected rows:") then
          table.insert(inline_lines, comment_prefix .. " " .. line)
          break
        end

        if line:match("^|.*|$") and not line:match("^%+%-") then
          if header_found then
            row_count = row_count + 1
          else
            header_found = true
          end
        end

        table.insert(inline_lines, comment_prefix .. " " .. line)

        if row_count >= max_rows then
          if result_lines[i + 1] and result_lines[i + 1]:match("^%+%-") then
            table.insert(inline_lines, comment_prefix .. " " .. result_lines[i + 1])
          end
          for _, l in ipairs(result_lines) do
            if l:match("^%(%d+ rows?%)$") then
              table.insert(inline_lines, comment_prefix .. " " .. l)
              break
            end
          end
          break
        end

        ::skip::
      end

      table.insert(inline_lines, 1, "")
      table.insert(results, { pos = q.finish, lines = inline_lines })
    end
  end

  for i = #results, 1, -1 do
    local r = results[i]
    local insert_pos = r.pos
    for j, line in ipairs(r.lines) do
      table.insert(cleaned_lines, insert_pos + j, line)
    end
  end

  vim.api.nvim_buf_set_lines(0, 0, -1, false, cleaned_lines)
  vim.api.nvim_echo({{"sqlazo: Updated " .. #queries .. " queries inline", "Normal"}}, true, {})
end

return M
