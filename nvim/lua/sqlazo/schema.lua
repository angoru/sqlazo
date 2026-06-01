-- sqlazo.nvim schema module
-- Schema introspection and autocomplete support

local M = {}

local parser = require("sqlazo.parser")
local runner = require("sqlazo.runner")
local config = require("sqlazo.config")

-- Schema cache keyed by connection URL
M.cache = {}

-- Get connection key from buffer header for cache key
function M.get_connection_key(lines)
  lines = lines or vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = parser.get_header(lines)

  local key = ""
  for _, line in ipairs(header_lines) do
    local trimmed = line:match("^%s*(.-)%s*$")
    if parser.is_header_line(line) then
      key = key .. trimmed
    end
  end
  return key
end

-- Fetch schema from database using CLI
function M.get(force_refresh)
  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = parser.get_header(all_lines)
  local profile = config.get().profile

  local cache_key = M.get_connection_key(all_lines) .. "|profile:" .. (profile or "") .. "|env"

  if not force_refresh and M.cache[cache_key] then
    return M.cache[cache_key]
  end

  local content = table.concat(header_lines, "\n") .. "\n\nSELECT 1;"
  local cmd = runner.get_cmd()
  if runner.uses_query_subcommand() then
    table.insert(cmd, "query")
  end
  table.insert(cmd, "--schema")
  if profile then
    table.insert(cmd, "--profile")
    table.insert(cmd, profile)
  end
  table.insert(cmd, "-")
  local output = vim.fn.system(cmd, content)
  local exit_code = vim.v.shell_error

  if exit_code ~= 0 then
    return nil, "Failed to fetch schema: " .. output
  end

  local ok, schema = pcall(vim.json.decode, output)
  if not ok then
    return nil, "Failed to parse schema JSON: " .. tostring(schema)
  end

  M.cache[cache_key] = schema
  return schema
end

-- Clear schema cache
function M.clear_cache()
  M.cache = {}
  vim.api.nvim_echo({{"sqlazo: Schema cache cleared", "Normal"}}, true, {})
end

-- Extract tables mentioned in query
function M.extract_tables_from_query(query_text)
  local tables = {}
  local upper_query = query_text:upper()

  local patterns = {
    "FROM%s+([%w_]+)",
    "JOIN%s+([%w_]+)",
    "INTO%s+([%w_]+)",
    "UPDATE%s+([%w_]+)",
  }

  for _, pattern in ipairs(patterns) do
    for table_name in upper_query:gmatch(pattern) do
      tables[table_name:lower()] = table_name
    end
  end

  return tables
end

local function trim(value)
  return (value or ""):match("^%s*(.-)%s*$")
end

local function normalize_sql(value)
  return ((value or ""):gsub("%s+", " "):gsub("^%s+", ""))
end

local function is_inside_string(value)
  local quote = nil
  local i = 1

  while i <= #value do
    local char = value:sub(i, i)
    local next_char = value:sub(i + 1, i + 1)

    if quote then
      if char == "\\" then
        i = i + 2
      elseif char == quote and next_char == quote then
        i = i + 2
      elseif char == quote then
        quote = nil
        i = i + 1
      else
        i = i + 1
      end
    elseif char == "'" or char == '"' or char == "`" then
      quote = char
      i = i + 1
    else
      i = i + 1
    end
  end

  return quote ~= nil
end

local function current_statement(sql)
  local statement = sql or ""
  local index = #statement

  while index > 0 do
    if statement:sub(index, index) == ";" then
      return statement:sub(index + 1)
    end
    index = index - 1
  end

  return statement
end

local function get_cursor_before(current_line_before)
  local row, col = unpack(vim.api.nvim_win_get_cursor(0))
  local lines = vim.api.nvim_buf_get_lines(0, 0, row, false)

  if #lines == 0 then
    return ""
  end

  lines[#lines] = current_line_before or lines[#lines]:sub(1, col)
  return table.concat(lines, "\n")
end

local function get_statement_at_cursor(current_line_before)
  local row, col = unpack(vim.api.nvim_win_get_cursor(0))
  local lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)

  if #lines == 0 then
    return ""
  end

  local before = current_line_before or lines[row]:sub(1, col)
  lines[row] = before .. lines[row]:sub(col + 1)

  local cursor_offset = #before
  for i = 1, row - 1 do
    cursor_offset = cursor_offset + #lines[i] + 1
  end

  local text = table.concat(lines, "\n")
  local start_index = 1
  local end_index = #text

  for i = cursor_offset, 1, -1 do
    if text:sub(i, i) == ";" then
      start_index = i + 1
      break
    end
  end

  for i = cursor_offset + 1, #text do
    if text:sub(i, i) == ";" then
      end_index = i - 1
      break
    end
  end

  return text:sub(start_index, end_index)
end

local function clean_table_name(table_name)
  return (table_name or ""):gsub("[`\"]", "")
end

local function base_table_name(table_name)
  local clean = clean_table_name(table_name)
  return clean:match("([%w_]+)$") or clean
end

local function get_schema_columns(schema, table_name)
  if not schema.columns or not table_name then
    return nil
  end

  local clean_name = clean_table_name(table_name)
  if schema.columns[clean_name] then
    return schema.columns[clean_name]
  end

  if schema.columns[table_name] then
    return schema.columns[table_name]
  end

  local lower_name = clean_name:lower()
  local lower_base_name = base_table_name(clean_name):lower()
  for name, columns in pairs(schema.columns) do
    local lower_schema_name = name:lower()
    if lower_schema_name == lower_name or base_table_name(name):lower() == lower_base_name then
      return columns
    end
  end

  return nil
end

local function each_schema_column(schema, callback)
  local seen_tables = {}

  for _, table_name in ipairs(schema.tables or {}) do
    local columns = get_schema_columns(schema, table_name)
    if columns then
      seen_tables[table_name:lower()] = true
      for _, col in ipairs(columns) do
        callback(col, table_name)
      end
    end
  end

  for table_name, columns in pairs(schema.columns or {}) do
    if not seen_tables[table_name:lower()] then
      for _, col in ipairs(columns) do
        callback(col, table_name)
      end
    end
  end
end

local function extract_table_aliases(statement)
  local aliases = {}
  local sql = normalize_sql(statement)
  local patterns = {
    "%f[%a]FROM%s+([%w_%.`\"]+)%s+([%w_]+)",
    "%f[%a]JOIN%s+([%w_%.`\"]+)%s+([%w_]+)",
    "%f[%a]UPDATE%s+([%w_%.`\"]+)%s+([%w_]+)",
    "%f[%a]INTO%s+([%w_%.`\"]+)%s+([%w_]+)",
  }

  for _, pattern in ipairs(patterns) do
    for table_name, alias in sql:gmatch(pattern) do
      local upper_alias = alias:upper()
      if upper_alias ~= "ON" and upper_alias ~= "WHERE" and upper_alias ~= "SET" and
          upper_alias ~= "VALUES" and upper_alias ~= "AS" then
        table_name = clean_table_name(table_name)
        aliases[alias] = table_name
        aliases[alias:lower()] = table_name
      end
    end
  end

  local as_patterns = {
    "%f[%a]FROM%s+([%w_%.`\"]+)%s+AS%s+([%w_]+)",
    "%f[%a]JOIN%s+([%w_%.`\"]+)%s+AS%s+([%w_]+)",
    "%f[%a]UPDATE%s+([%w_%.`\"]+)%s+AS%s+([%w_]+)",
    "%f[%a]INTO%s+([%w_%.`\"]+)%s+AS%s+([%w_]+)",
  }

  for _, pattern in ipairs(as_patterns) do
    for table_name, alias in sql:gmatch(pattern) do
      table_name = clean_table_name(table_name)
      aliases[alias] = table_name
      aliases[alias:lower()] = table_name
    end
  end

  return aliases
end

local function referenced_tables(statement)
  local tables = {}
  local sql = normalize_sql(statement)
  local patterns = {
    "%f[%a]FROM%s+([%w_%.`\"]+)",
    "%f[%a]JOIN%s+([%w_%.`\"]+)",
    "%f[%a]UPDATE%s+([%w_%.`\"]+)",
    "%f[%a]INTO%s+([%w_%.`\"]+)",
  }

  for _, pattern in ipairs(patterns) do
    for table_name in sql:gmatch(pattern) do
      local clean = clean_table_name(table_name)
      if clean:upper() ~= "SELECT" then
        table.insert(tables, clean)
      end
    end
  end

  return tables
end

local function insert_table_for_column_list(statement)
  local sql = normalize_sql(statement)
  local table_name = sql:match("%f[%a]INSERT%s+INTO%s+([%w_%.`\"]+)%s*%([^%)]*$")
  if table_name then
    return clean_table_name(table_name)
  end
  return nil
end

local function table_items(schema)
  local items = {}
  for _, table_name in ipairs(schema.tables or {}) do
    table.insert(items, {
      label = table_name,
      insertText = table_name,
      filterText = table_name,
      kind = 7,
      detail = "Table",
      sortText = "0_" .. table_name,
    })
  end
  return items
end

local function column_items(schema, statement, table_name, opts)
  opts = opts or {}
  local items = {}
  local seen = {}
  local tables = {}

  local function add_column(col, source_table)
    local label = col.name
    if opts.qualify and source_table then
      label = base_table_name(source_table) .. "." .. col.name
    end

    local key = label:lower()
    if not seen[key] then
      seen[key] = true
      table.insert(items, {
        label = label,
        insertText = label,
        kind = 5,
        detail = source_table and (base_table_name(source_table) .. " | " .. col.type) or col.type,
      })
    end
  end

  if table_name then
    table.insert(tables, table_name)
  else
    tables = referenced_tables(statement)
  end

  if #tables == 0 then
    each_schema_column(schema, add_column)
    return items
  end

  for _, name in ipairs(tables) do
    for _, col in ipairs(get_schema_columns(schema, name) or {}) do
      add_column(col, name)
    end
  end

  return items
end

-- Determine SQL context based on cursor position
function M.get_sql_context(cursor_before)
  local statement = current_statement(cursor_before)
  local upper_before = normalize_sql(statement):upper()

  if is_inside_string(statement) then
    return "none"
  end

  if cursor_before:match("([%w_]+)%.$") then
    return "column_qualified"
  end

  if upper_before:match("INSERT%s+INTO%s+[%w_%.`\"]+%s*%([^%)]*$") then
    return "insert_column"
  end

  local table_contexts = {
    "FROM%s+[%w_]*$", "FROM%s+[%w_]+%s*,%s*[%w_]*$", "JOIN%s+[%w_]*$",
    "INTO%s+[%w_]*$", "UPDATE%s+[%w_]*$", "TABLE%s+[%w_]*$",
    "TRUNCATE%s+[%w_]*$", "DESC%s+[%w_]*$", "DESCRIBE%s+[%w_]*$",
  }

  for _, pattern in ipairs(table_contexts) do
    if upper_before:match(pattern) then
      return "table"
    end
  end

  local column_contexts = {
    "SELECT%s+[%w_]*$", "SELECT%s+.+,%s*[%w_]*$", "WHERE%s+[%w_]*$", "WHERE%s+.+AND%s+[%w_]*$",
    "WHERE%s+.+OR%s+$", "SET%s+$", "SET%s+.+,%s*$", "ORDER%s+BY%s+$",
    "ORDER%s+BY%s+.+,%s*$", "GROUP%s+BY%s+$", "GROUP%s+BY%s+.+,%s*$",
    "HAVING%s+$", "ON%s+$", "AND%s+$", "OR%s+$",
  }

  for _, pattern in ipairs(column_contexts) do
    if upper_before:match(pattern) then
      return "column"
    end
  end

  if upper_before:match("VALUES%s*%([^%)]*$") or
      upper_before:match("LIMIT%s+[%d%s,]*$") or
      upper_before:match("OFFSET%s+%d*%s*$") then
    return "none"
  end

  local trailing_column_clauses = {
    "%f[%a]WHERE%f[%A].-$",
    "%f[%a]ORDER%s+BY%f[%A].-$",
    "%f[%a]GROUP%s+BY%f[%A].-$",
    "%f[%a]HAVING%f[%A].-$",
    "%f[%a]ON%f[%A].-$",
    "%f[%a]SET%f[%A].-$",
  }

  for _, pattern in ipairs(trailing_column_clauses) do
    if upper_before:match(pattern) then
      return "column"
    end
  end

  if upper_before:match("%f[%a]SELECT%s+.-$") and not upper_before:match("%f[%a]FROM%f[%A]") then
    return "column"
  end

  return "mixed"
end

-- Get nvim-cmp compatible completion source
function M.get_cmp_source()
  local source = {}

  source.new = function()
    return setmetatable({}, { __index = source })
  end

  source.get_trigger_characters = function()
    return { ".", " ", "\t", "\n", "," }
  end

  source.is_available = function()
    local ft = vim.bo.filetype
    local map = config.get().comment_prefix_by_filetype or {}
    return map[ft] ~= nil
  end

  source.complete = function(self, params, callback)
    local schema = M.get()
    if not schema then
      callback({ items = {}, isIncomplete = false })
      return
    end

    local items = {}
    local current_line_before = params.context and params.context.cursor_before_line or nil
    local cursor_before = get_cursor_before(current_line_before)
    local statement = current_statement(cursor_before)
    local full_statement = get_statement_at_cursor(current_line_before)
    local context = M.get_sql_context(cursor_before)
    local table_prefix = cursor_before:match("([%w_]+)%.$")

    if table_prefix then
      local aliases = extract_table_aliases(full_statement)
      local table_name = aliases[table_prefix] or aliases[table_prefix:lower()] or table_prefix
      items = column_items(schema, full_statement, table_name)
    elseif context == "table" then
      items = table_items(schema)
    elseif context == "column" then
      local has_from = #referenced_tables(full_statement) > 0
      items = column_items(schema, full_statement, nil, { qualify = not has_from })
    elseif context == "insert_column" then
      items = column_items(schema, full_statement, insert_table_for_column_list(full_statement))
    elseif context == "none" then
      items = {}
    else
      items = table_items(schema)
    end

    callback({ items = items, isIncomplete = false })
  end

  source.get_debug_name = function()
    return "sqlazo"
  end

  return source
end

local function patch_cmp_sources(cmp)
  local current = cmp.get_config().sources or {}
  local patched = {}
  local found = false

  for _, source_config in ipairs(current) do
    local copy = vim.tbl_deep_extend("force", {}, source_config)
    if copy.name == "sqlazo" then
      copy.keyword_length = 0
      copy.priority = math.max(copy.priority or 0, 1000)
      found = true
    end
    table.insert(patched, copy)
  end

  if not found then
    table.insert(patched, { name = "sqlazo", keyword_length = 0, priority = 1000 })
  end

  cmp.setup({ sources = patched })
end

local function should_trigger_empty_completion()
  local ft = vim.bo.filetype
  local map = config.get().comment_prefix_by_filetype or {}
  if map[ft] == nil then
    return false
  end

  local cursor_before = get_cursor_before()
  local context = M.get_sql_context(cursor_before)

  return cursor_before:match("([%w_]+)%.$") ~= nil or
      context == "column" or
      context == "table" or
      context == "insert_column"
end

local function setup_empty_completion_trigger(cmp)
  local group = vim.api.nvim_create_augroup("sqlazo_cmp_empty_completion", { clear = true })
  vim.api.nvim_create_autocmd("TextChangedI", {
    group = group,
    callback = function()
      if vim.fn.mode() ~= "i" or not should_trigger_empty_completion() then
        return
      end

      cmp.complete({
        config = {
          sources = {
            { name = "sqlazo", keyword_length = 0, priority = 1000 },
          },
        },
      })
    end,
  })
end

-- Register with nvim-cmp
function M.setup_cmp()
  local ok, cmp = pcall(require, "cmp")
  if not ok then
    return false
  end

  cmp.register_source("sqlazo", M.get_cmp_source().new())
  patch_cmp_sources(cmp)
  setup_empty_completion_trigger(cmp)
  return true
end

return M
