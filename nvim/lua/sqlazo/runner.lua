-- sqlazo.nvim runner

local M = {}

local config = require("sqlazo.config")
local parser = require("sqlazo.parser")
local results = require("sqlazo.results")

local supports_query_subcommand
local supports_json_meta

local function python_cmd()
  local cmd = vim.split(config.get().python_cmd, "%s+", { trimempty = true })
  table.insert(cmd, "-m")
  table.insert(cmd, "sqlazo")
  return cmd
end

function M.get_cmd()
  local cfg = config.get()
  if cfg.prefer_python or cfg.python_cmd ~= "python" or vim.fn.executable("sqlazo") ~= 1 then
    return python_cmd()
  end
  return { "sqlazo" }
end

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

  local cmd = M.get_cmd()
  if M.uses_query_subcommand() then
    table.insert(cmd, "query")
  end
  table.insert(cmd, "--help")
  local help = vim.fn.system(cmd)
  supports_json_meta = vim.v.shell_error == 0 and help:match("json%-meta") ~= nil
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

local function execute(content, format, profile)
  local output = vim.fn.system(build_query_cmd(format, profile), content)
  return output, vim.v.shell_error
end

local function execute_result(content, profile)
  if not M.supports_json_meta() then
    local output, exit_code = execute(content, config.get().format, profile)
    return { raw_output = output }, output, exit_code
  end

  local output, exit_code = execute(content, "json-meta", profile)
  if exit_code ~= 0 then
    return nil, output, exit_code
  end

  local ok, decoded = pcall(vim.json.decode, output)
  if not ok then
    return nil, "Failed to parse sqlazo output: " .. tostring(decoded), 1
  end

  return decoded, nil, 0
end

local function create_result_buffer()
  local existing = vim.fn.bufnr("sqlazo://result")
  if existing ~= -1 and vim.api.nvim_buf_is_valid(existing) then
    local win = vim.fn.bufwinid(existing)
    if win ~= -1 then
      vim.api.nvim_set_current_win(win)
    else
      vim.cmd("botright split")
      vim.api.nvim_win_set_buf(0, existing)
    end
    return existing
  end

  vim.cmd("botright split")
  local buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_win_set_buf(0, buf)
  vim.api.nvim_buf_set_name(buf, "sqlazo://result")
  vim.api.nvim_buf_set_option(buf, "buftype", "nofile")
  vim.api.nvim_buf_set_option(buf, "bufhidden", "wipe")
  vim.api.nvim_buf_set_option(buf, "swapfile", false)
  return buf
end

local function render_into_buffer(buf, content, profile)
  local result, error_message, exit_code = execute_result(content, profile)
  if exit_code == 0 then
    results.set_result(buf, result)
    results.setup_keymaps(buf)
    vim.api.nvim_echo({ { "sqlazo: Query executed", "Normal" } }, true, {})
  else
    results.set_error(buf, error_message)
    vim.api.nvim_echo({ { "sqlazo: Query failed", "ErrorMsg" } }, true, {})
  end
end

local function sql_literal(value)
  if value == vim.NIL or value == nil then
    return "NULL"
  end
  if type(value) == "number" then
    return tostring(value)
  end
  if type(value) == "boolean" then
    return value and "1" or "0"
  end
  return "'" .. tostring(value):gsub("'", "''") .. "'"
end

local function predicate(column, value)
  if value == vim.NIL or value == nil then
    return column .. " IS NULL"
  end
  return column .. " = " .. sql_literal(value)
end

local function add_filter_to_query(lines, filter)
  local query = table.concat(lines, "\n")
  local semicolon = query:match(";%s*$") ~= nil
  query = query:gsub(";%s*$", "")

  local upper = query:upper()
  local terminal_at
  for _, pattern in ipairs({
    "%f[%a]GROUP%s+BY%f[%A]",
    "%f[%a]HAVING%f[%A]",
    "%f[%a]ORDER%s+BY%f[%A]",
    "%f[%a]LIMIT%f[%A]",
    "%f[%a]OFFSET%f[%A]",
  }) do
    local start = upper:find(pattern)
    if start and (not terminal_at or start < terminal_at) then
      terminal_at = start
    end
  end

  local before = terminal_at and query:sub(1, terminal_at - 1) or query
  local after = terminal_at and query:sub(terminal_at) or ""
  local connector = before:upper():match("%f[%a]WHERE%f[%A]") and " AND " or " WHERE "
  local updated = before:gsub("%s+$", "") .. connector .. filter

  if after ~= "" then
    updated = updated .. " " .. after:gsub("^%s+", "")
  end
  if semicolon then
    updated = updated .. ";"
  end

  return vim.split(updated, "\n", { plain = true })
end

function M.filter_by_selected_value(result_buf)
  result_buf = result_buf or vim.api.nvim_get_current_buf()
  local source_buf = vim.b[result_buf].sqlazo_source_buf
  local query_start = vim.b[result_buf].sqlazo_query_start
  local query_end = vim.b[result_buf].sqlazo_query_end
  local header_lines = vim.b[result_buf].sqlazo_header_lines or {}
  local profile = vim.b[result_buf].sqlazo_profile
  local cell = results.selected_cell(result_buf)

  if not source_buf or not vim.api.nvim_buf_is_valid(source_buf) or not query_start or not query_end then
    vim.api.nvim_echo({ { "sqlazo: Source query unavailable", "WarningMsg" } }, true, {})
    return
  end
  if not cell then
    vim.api.nvim_echo({ { "sqlazo: No selected result cell", "WarningMsg" } }, true, {})
    return
  end

  local query_lines = vim.api.nvim_buf_get_lines(source_buf, query_start - 1, query_end, false)
  local updated = add_filter_to_query(query_lines, predicate(cell.column, cell.value))
  vim.api.nvim_buf_set_lines(source_buf, query_start - 1, query_end, false, updated)
  vim.b[result_buf].sqlazo_query_end = query_start + #updated - 1

  vim.api.nvim_echo({ { "sqlazo: Added filter " .. cell.column .. " = " .. tostring(cell.value), "Normal" } }, true, {})
  render_into_buffer(result_buf, parser.build_content(header_lines, updated), profile)
end

function M.run(opts)
  opts = vim.tbl_deep_extend("force", {}, config.get(), opts or {})

  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = parser.get_header(all_lines)
  local query_lines, query_start, query_end = parser.get_query_at_cursor(all_lines)
  local source_buf = vim.api.nvim_get_current_buf()

  if #query_lines == 0 then
    vim.api.nvim_echo({ { "sqlazo: No query found at cursor", "WarningMsg" } }, true, {})
    return
  end

  local content = parser.build_content(header_lines, query_lines)
  local function run_query()
    local buf = create_result_buffer()
    vim.b[buf].sqlazo_source_buf = source_buf
    vim.b[buf].sqlazo_query_start = query_start
    vim.b[buf].sqlazo_query_end = query_end
    vim.b[buf].sqlazo_header_lines = header_lines
    vim.b[buf].sqlazo_profile = opts.profile
    render_into_buffer(buf, content, opts.profile)
  end

  if config.get().safe_mode then
    local is_destructive, keyword = config.is_destructive_query(content)
    if is_destructive then
      config.confirm_destructive(keyword, run_query)
      return
    end
  end

  run_query()
end

return M
