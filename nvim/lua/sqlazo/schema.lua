-- sqlazo.nvim schema module
-- Schema introspection and autocomplete support

local M = {}

local parser = require("sqlazo.parser")
local runner = require("sqlazo.runner")

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

  if #header_lines == 0 then
    return nil, "No connection header found in buffer"
  end

  local cache_key = M.get_connection_key(all_lines)

  if not force_refresh and M.cache[cache_key] then
    return M.cache[cache_key]
  end

  local content = table.concat(header_lines, "\n") .. "\n\nSELECT 1;"
  local cmd = runner.get_cmd() .. " --schema -"
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

-- Determine SQL context based on cursor position
function M.get_sql_context(cursor_before)
  local upper_before = cursor_before:upper()

  if cursor_before:match("([%w_]+)%.$") then
    return "column_qualified"
  end

  local table_contexts = {
    "FROM%s+$", "FROM%s+[%w_]+%s*,%s*$", "JOIN%s+$", "INTO%s+$",
    "UPDATE%s+$", "TABLE%s+$", "TRUNCATE%s+$", "DESC%s+$", "DESCRIBE%s+$",
  }

  for _, pattern in ipairs(table_contexts) do
    if upper_before:match(pattern) then
      return "table"
    end
  end

  local column_contexts = {
    "SELECT%s+$", "SELECT%s+.+,%s*$", "WHERE%s+$", "WHERE%s+.+AND%s+$",
    "WHERE%s+.+OR%s+$", "SET%s+$", "SET%s+.+,%s*$", "ORDER%s+BY%s+$",
    "ORDER%s+BY%s+.+,%s*$", "GROUP%s+BY%s+$", "GROUP%s+BY%s+.+,%s*$",
    "HAVING%s+$", "ON%s+$", "AND%s+$", "OR%s+$",
  }

  for _, pattern in ipairs(column_contexts) do
    if upper_before:match(pattern) then
      return "column"
    end
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
    return ft == "sql" or ft == "mysql"
  end

  source.complete = function(self, params, callback)
    local schema = M.get()
    if not schema then
      callback({ items = {}, isIncomplete = false })
      return
    end

    local items = {}
    local cursor_before = params.context.cursor_before_line
    local context = M.get_sql_context(cursor_before)
    local table_prefix = cursor_before:match("([%w_]+)%.$")

    if table_prefix then
      if schema.columns and schema.columns[table_prefix] then
        for _, col in ipairs(schema.columns[table_prefix]) do
          table.insert(items, {
            label = col.name,
            kind = 5,
            detail = col.type,
          })
        end
      end
    elseif context == "table" then
      if schema.tables then
        for _, table_name in ipairs(schema.tables) do
          table.insert(items, {
            label = table_name,
            kind = 7,
            detail = "Table",
          })
        end
      end
    else
      if schema.tables then
        for _, table_name in ipairs(schema.tables) do
          table.insert(items, {
            label = table_name,
            kind = 7,
            detail = "Table",
          })
        end
      end
    end

    callback({ items = items, isIncomplete = false })
  end

  source.get_debug_name = function()
    return "sqlazo"
  end

  return source
end

-- Register with nvim-cmp
function M.setup_cmp()
  local ok, cmp = pcall(require, "cmp")
  if not ok then
    return false
  end

  cmp.register_source("sqlazo", M.get_cmp_source().new())
  return true
end

return M
