-- sqlazo.nvim autocomplete

local M = {}

local config = require("sqlazo.config")
local parser = require("sqlazo.parser")
local runner = require("sqlazo.runner")

M.cache = {}

local function normalize_sql(value)
  return ((value or ""):gsub("%s+", " "):gsub("^%s+", ""))
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

local function cache_key(lines)
  local parts = {}
  for _, line in ipairs(parser.get_header(lines)) do
    table.insert(parts, line)
  end
  return table.concat(parts, "\n")
end

local loading = {}

local function build_schema_content(lines)
  return table.concat(parser.get_header(lines), "\n") .. "\n\nSELECT 1;"
end

local function schema_cmd()
  local cmd = runner.get_cmd()
  table.insert(cmd, "query")
  table.insert(cmd, "--schema")
  table.insert(cmd, "-")
  return cmd
end

-- Load the schema without blocking the editor. The Rust binary connects to the
-- database, which can take a while (especially over the network), so we never
-- run it synchronously on the typing hot path.
local function load_async(key, content, on_done)
  if M.cache[key] then
    if on_done then
      on_done(M.cache[key])
    end
    return
  end
  if loading[key] then
    return
  end
  loading[key] = true

  local function finish(code, output)
    loading[key] = nil
    if code == 0 and output and output ~= "" then
      local ok, schema = pcall(vim.json.decode, output)
      if ok and type(schema) == "table" then
        M.cache[key] = schema
      end
    end
    if on_done then
      on_done(M.cache[key])
    end
  end

  local cmd = schema_cmd()

  if vim.system then
    vim.system(cmd, { stdin = content, text = true }, function(obj)
      vim.schedule(function()
        finish(obj.code, obj.stdout)
      end)
    end)
    return
  end

  local chunks = {}
  local job = vim.fn.jobstart(cmd, {
    stdout_buffered = true,
    on_stdout = function(_, data)
      if data then
        for _, line in ipairs(data) do
          table.insert(chunks, line)
        end
      end
    end,
    on_exit = function(_, code)
      finish(code, table.concat(chunks, "\n"))
    end,
  })
  if job <= 0 then
    loading[key] = nil
    if on_done then
      on_done(nil)
    end
    return
  end
  vim.fn.chansend(job, content)
  vim.fn.chanclose(job, "stdin")
end

-- Returns the cached schema if available; otherwise kicks off an async load and
-- returns nil. When provided, on_ready fires once the load completes.
function M.get(force_refresh, on_ready)
  local lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local key = cache_key(lines)
  if force_refresh then
    M.cache[key] = nil
  end
  if M.cache[key] then
    return M.cache[key]
  end
  load_async(key, build_schema_content(lines), on_ready)
  return nil
end

-- Warm the cache ahead of time (e.g. on buffer enter) so the first completion
-- is served synchronously from cache.
function M.prime()
  local lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local key = cache_key(lines)
  load_async(key, build_schema_content(lines))
end

local function clean_name(name)
  return (name or ""):gsub("[`\"]", "")
end

local function base_name(name)
  local clean = clean_name(name)
  return clean:match("([%w_]+)$") or clean
end

local function schema_columns(schema, table_name)
  if not schema.columns or not table_name then
    return nil
  end

  local clean = clean_name(table_name)
  if schema.columns[clean] then
    return schema.columns[clean]
  end

  local lower = clean:lower()
  local lower_base = base_name(clean):lower()
  for name, columns in pairs(schema.columns) do
    if name:lower() == lower or base_name(name):lower() == lower_base then
      return columns
    end
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

local function referenced_tables(statement)
  local tables = {}
  local sql = normalize_sql(statement):upper()
  for _, pattern in ipairs({
    "%f[%a]FROM%s+([%w_%.`\"]+)",
    "%f[%a]JOIN%s+([%w_%.`\"]+)",
  }) do
    for table_name in sql:gmatch(pattern) do
      local clean = clean_name(table_name)
      table.insert(tables, clean)
    end
  end
  return tables
end

local function table_aliases(statement)
  local aliases = {}
  local sql = normalize_sql(statement):upper()
  for _, pattern in ipairs({
    "%f[%a]FROM%s+([%w_%.`\"]+)%s+([%w_]+)",
    "%f[%a]JOIN%s+([%w_%.`\"]+)%s+([%w_]+)",
    "%f[%a]FROM%s+([%w_%.`\"]+)%s+AS%s+([%w_]+)",
    "%f[%a]JOIN%s+([%w_%.`\"]+)%s+AS%s+([%w_]+)",
  }) do
    for table_name, alias in sql:gmatch(pattern) do
      local upper = alias:upper()
      if upper ~= "WHERE" and upper ~= "ON" and upper ~= "JOIN" and upper ~= "ORDER" and upper ~= "GROUP" then
        aliases[alias] = clean_name(table_name)
        aliases[alias:lower()] = clean_name(table_name)
      end
    end
  end
  return aliases
end

local function column_items(schema, statement, table_name)
  local tables = table_name and { table_name } or referenced_tables(statement)
  local seen = {}
  local items = {}

  for _, name in ipairs(tables) do
    for _, col in ipairs(schema_columns(schema, name) or {}) do
      local key = col.name:lower()
      if not seen[key] then
        seen[key] = true
        local is_key = type(col.key) == "string" and col.key ~= ""
        local detail = base_name(name) .. " | " .. (col.type or "")
        if is_key then
          detail = detail .. " | " .. col.key
        end
        table.insert(items, {
          label = col.name,
          insertText = col.name,
          kind = 5,
          detail = detail,
          sortText = (is_key and "0_" or "1_") .. col.name,
        })
      end
    end
  end

  return items
end

local function completion_context(cursor_before)
  local upper = normalize_sql(current_statement(cursor_before)):upper()
  if cursor_before:match("([%w_]+)%.$") then
    return "qualified"
  end
  if upper:match("%f[%a]FROM%s+[%w_]*$") or
    upper:match("%f[%a]JOIN%s+[%w_]*$") or
    upper:match(",%s*[%w_]*$") and upper:match("%f[%a]FROM%f[%A]") then
    return "table"
  end
  if upper:match("%f[%a]WHERE%f[%A].*$") or
    upper:match("%f[%a]AND%s+[%w_]*$") or
    upper:match("%f[%a]OR%s+[%w_]*$") or
    upper:match("%f[%a]ON%f[%A].*$") or
    upper:match("%f[%a]GROUP%s+BY%f[%A].*$") or
    upper:match("%f[%a]HAVING%f[%A].*$") or
    upper:match("%f[%a]ORDER%s+BY%f[%A].*$") or
    (upper:match("^SELECT%f[%A].*$") and not upper:match("%f[%a]FROM%f[%A]")) then
    return "column"
  end
  return "none"
end

function M.get_cmp_source()
  local source = {}

  source.new = function()
    return setmetatable({}, { __index = source })
  end

  source.get_trigger_characters = function()
    return { " ", ".", "," }
  end

  source.is_available = function()
    return (config.get().comment_prefix_by_filetype or {})[vim.bo.filetype] ~= nil
  end

  source.complete = function(_, params, callback)
    local bufnr = vim.api.nvim_get_current_buf()
    local schema = M.get(false, function(loaded)
      -- The schema finished loading asynchronously; re-trigger so the now-warm
      -- cache is used, as long as we are still typing in the same buffer.
      if loaded and vim.api.nvim_get_current_buf() == bufnr and vim.fn.mode() == "i" then
        local cmp_ok, cmp = pcall(require, "cmp")
        if cmp_ok then
          cmp.complete()
        end
      end
    end)
    if not schema then
      -- isIncomplete keeps cmp asking until the async load populates the cache.
      callback({ items = {}, isIncomplete = true })
      return
    end

    local current_line_before = params.context and params.context.cursor_before_line or nil
    local cursor_before = get_cursor_before(current_line_before)
    local statement = get_statement_at_cursor(current_line_before)
    local context = completion_context(cursor_before)
    local items = {}

    if context == "table" then
      items = table_items(schema)
    elseif context == "qualified" then
      local prefix = cursor_before:match("([%w_]+)%.$")
      local aliases = table_aliases(statement)
      items = column_items(schema, statement, aliases[prefix] or aliases[prefix:lower()] or prefix)
    elseif context == "column" then
      items = column_items(schema, statement)
    end

    callback({ items = items, isIncomplete = false })
  end

  source.get_debug_name = function()
    return "sqlazo"
  end

  return source
end

local function patch_cmp_source(cmp)
  local sources = vim.deepcopy(cmp.get_config().sources or {})
  local found = false
  for _, source in ipairs(sources) do
    if source.name == "sqlazo" then
      source.keyword_length = 0
      source.priority = math.max(source.priority or 0, 1000)
      found = true
    end
  end
  if not found then
    table.insert(sources, { name = "sqlazo", keyword_length = 0, priority = 1000 })
  end
  cmp.setup({ sources = sources })
end

local function is_sql_buffer()
  return (config.get().comment_prefix_by_filetype or {})[vim.bo.filetype] ~= nil
end

local function should_trigger_completion()
  if not is_sql_buffer() then
    return false
  end

  local context = completion_context(get_cursor_before())
  return context == "table" or context == "column" or context == "qualified"
end

local debounce_timer

local function setup_context_trigger(cmp)
  local group = vim.api.nvim_create_augroup("sqlazo_cmp_context_trigger", { clear = true })
  vim.api.nvim_create_autocmd("TextChangedI", {
    group = group,
    callback = function()
      -- Debounce: only evaluate the (regex-heavy) context and ask cmp to
      -- complete after the user pauses, instead of on every keystroke.
      if debounce_timer then
        debounce_timer:stop()
        debounce_timer:close()
        debounce_timer = nil
      end

      local delay = config.get().schema_debounce_ms or 100
      debounce_timer = vim.defer_fn(function()
        debounce_timer = nil
        if vim.fn.mode() ~= "i" or not should_trigger_completion() then
          return
        end

        cmp.complete({
          config = {
            sources = {
              { name = "sqlazo", keyword_length = 0, priority = 1000 },
            },
          },
        })
      end, delay)
    end,
  })
end

local function setup_prime_trigger()
  if config.get().schema_prime_on_enter == false then
    return
  end
  local group = vim.api.nvim_create_augroup("sqlazo_cmp_prime", { clear = true })
  vim.api.nvim_create_autocmd({ "BufEnter", "FileType" }, {
    group = group,
    callback = function()
      if is_sql_buffer() then
        M.prime()
      end
    end,
  })
end

function M.setup_cmp()
  local ok, cmp = pcall(require, "cmp")
  if not ok then
    return false
  end

  cmp.register_source("sqlazo", M.get_cmp_source().new())
  patch_cmp_source(cmp)
  setup_context_trigger(cmp)
  setup_prime_trigger()
  return true
end

-- Exposed for tests only.
M._internal = {
  completion_context = completion_context,
  referenced_tables = referenced_tables,
  table_aliases = table_aliases,
  column_items = column_items,
  table_items = table_items,
}

return M
