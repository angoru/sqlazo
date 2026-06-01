-- sqlazo.nvim results module
-- Structured result rendering and navigation helpers

local M = {}

local ns = vim.api.nvim_create_namespace("sqlazo_result")

local function stringify(value)
  if value == vim.NIL or value == nil then
    return "NULL"
  end
  return tostring(value)
end

local function csv_escape(value)
  local text = stringify(value)
  if text:find('[,"\n]') then
    return '"' .. text:gsub('"', '""') .. '"'
  end
  return text
end

local function cell_bounds(buf, row, col)
  local meta = vim.b[buf].sqlazo_result_meta
  if not meta or not meta.widths then
    return nil
  end

  local line = meta.first_data_line + row - 1
  local start_col = 1
  for i = 1, col - 1 do
    start_col = start_col + meta.widths[i] + 3
  end
  return line, start_col, start_col + meta.widths[col] + 1
end

local function clamp_selection(buf)
  local result = vim.b[buf].sqlazo_result
  local meta = vim.b[buf].sqlazo_result_meta
  if not result or result.raw_output or not meta then
    return nil
  end

  local row_count = #(result.rows or {})
  local col_count = #(result.columns or {})
  if row_count == 0 or col_count == 0 then
    return nil
  end

  local selection = vim.b[buf].sqlazo_selection or { row = 1, col = 1 }
  selection.row = math.max(1, math.min(selection.row, row_count))
  selection.col = math.max(1, math.min(selection.col, col_count))
  vim.b[buf].sqlazo_selection = selection
  return selection
end

function M.highlight_selection(buf)
  vim.api.nvim_buf_clear_namespace(buf, ns, 0, -1)
  local selection = clamp_selection(buf)
  if not selection then
    return
  end

  local line, start_col, end_col = cell_bounds(buf, selection.row, selection.col)
  if not line then
    return
  end

  vim.api.nvim_buf_add_highlight(buf, ns, "Visual", line - 1, start_col, end_col)
  local current_win = vim.api.nvim_get_current_win()
  if vim.api.nvim_win_get_buf(current_win) == buf then
    pcall(vim.api.nvim_win_set_cursor, current_win, { line, start_col })
  end
end

function M.move_selection(buf, drow, dcol)
  local selection = clamp_selection(buf)
  if not selection then
    vim.api.nvim_echo({{"sqlazo: Structured result navigation unavailable", "WarningMsg"}}, true, {})
    return
  end

  selection.row = selection.row + drow
  selection.col = selection.col + dcol
  vim.b[buf].sqlazo_selection = selection
  M.highlight_selection(buf)
end

function M.copy_cell(buf)
  local result = vim.b[buf].sqlazo_result
  local selection = clamp_selection(buf)
  if not result or not selection then
    return
  end
  local value = stringify(result.rows[selection.row][selection.col])
  vim.fn.setreg("+", value)
  vim.fn.setreg('"', value)
  vim.api.nvim_echo({{"sqlazo: Copied cell", "Normal"}}, true, {})
end

function M.copy_row(buf)
  local result = vim.b[buf].sqlazo_result
  local selection = clamp_selection(buf)
  if not result or not selection then
    return
  end
  local header = {}
  for _, column in ipairs(result.columns or {}) do
    table.insert(header, csv_escape(column))
  end
  local values = {}
  for _, value in ipairs(result.rows[selection.row]) do
    table.insert(values, csv_escape(value))
  end
  local text = table.concat(header, ",") .. "\n" .. table.concat(values, ",")
  vim.fn.setreg("+", text)
  vim.fn.setreg('"', text)
  vim.api.nvim_echo({{"sqlazo: Copied row as CSV", "Normal"}}, true, {})
end

function M.copy_column(buf)
  local result = vim.b[buf].sqlazo_result
  local selection = clamp_selection(buf)
  if not result or not selection then
    return
  end
  local values = { csv_escape(result.columns[selection.col]) }
  for _, row in ipairs(result.rows or {}) do
    table.insert(values, csv_escape(row[selection.col]))
  end
  local text = table.concat(values, "\n")
  vim.fn.setreg("+", text)
  vim.fn.setreg('"', text)
  vim.api.nvim_echo({{"sqlazo: Copied column as CSV", "Normal"}}, true, {})
end

function M.to_csv(result)
  if not result or result.raw_output or not result.is_select then
    return nil
  end

  local lines = {}
  local header = {}
  for _, column in ipairs(result.columns or {}) do
    table.insert(header, csv_escape(column))
  end
  table.insert(lines, table.concat(header, ","))

  for _, row in ipairs(result.rows or {}) do
    local values = {}
    for _, value in ipairs(row) do
      table.insert(values, csv_escape(value))
    end
    table.insert(lines, table.concat(values, ","))
  end

  return table.concat(lines, "\n")
end

function M.export_csv(buf)
  local result = vim.b[buf].sqlazo_result
  local csv = M.to_csv(result)
  if not csv then
    vim.api.nvim_echo({{"sqlazo: CSV export requires structured SELECT results", "WarningMsg"}}, true, {})
    return
  end

  local default_name = "sqlazo-result.csv"
  local source_buf = vim.b[buf].sqlazo_source_buf
  if source_buf and vim.api.nvim_buf_is_valid(source_buf) then
    local source_name = vim.api.nvim_buf_get_name(source_buf)
    if source_name ~= "" then
      default_name = vim.fn.fnamemodify(source_name, ":r") .. ".csv"
    end
  end

  vim.ui.input({ prompt = "Export CSV: ", default = default_name }, function(path)
    if not path or path == "" then
      return
    end
    vim.fn.writefile(vim.split(csv, "\n", { plain = true }), path)
    vim.api.nvim_echo({{"sqlazo: Exported CSV to " .. path, "Normal"}}, true, {})
  end)
end

local function connection_label(result)
  local metadata = result.metadata or {}
  local connection = metadata.connection or {}
  local db_type = connection.db_type or "db"
  local host = connection.host or "local"
  local database = connection.database

  if database and database ~= "" then
    return db_type .. " " .. database .. "@" .. host
  end
  return db_type .. " " .. host
end

function M.render_lines(result)
  if result.raw_output then
    local lines = {}
    local metadata = result.metadata or {}
    local mode = metadata.mode or "legacy"
    table.insert(lines, "sqlazo: " .. mode .. " output")
    table.insert(lines, string.rep("-", 72))
    table.insert(lines, "")
    for _, line in ipairs(vim.split(result.raw_output, "\n", { trimempty = false })) do
      table.insert(lines, line)
    end
    return lines
  end

  local lines = {}
  local metadata = result.metadata or {}
  local duration = metadata.duration_ms and (tostring(metadata.duration_ms) .. " ms") or "unknown"
  local row_count = result.row_count or #(result.rows or {})

  table.insert(lines, "sqlazo: " .. connection_label(result) .. " | " .. row_count .. " rows | " .. duration)
  table.insert(lines, string.rep("-", math.max(72, #lines[1])))
  table.insert(lines, "")

  if not result.is_select then
    local affected = result.affected_rows or 0
    local line = "Affected rows: " .. affected
    if result.last_insert_id then
      line = line .. ", Last insert ID: " .. result.last_insert_id
    end
    table.insert(lines, line)
    return lines
  end

  local columns = result.columns or {}
  local rows = result.rows or {}
  if #columns == 0 then
    table.insert(lines, "(No results)")
    return lines
  end

  local widths = {}
  for i, column in ipairs(columns) do
    widths[i] = #tostring(column)
  end

  for _, row in ipairs(rows) do
    for i, value in ipairs(row) do
      widths[i] = math.max(widths[i] or 0, #stringify(value))
    end
  end

  local function separator()
    local parts = {}
    for _, width in ipairs(widths) do
      table.insert(parts, string.rep("-", width + 2))
    end
    return "+" .. table.concat(parts, "+") .. "+"
  end

  local function row_line(values)
    local parts = {}
    for i, value in ipairs(values) do
      table.insert(parts, " " .. string.format("%-" .. widths[i] .. "s", stringify(value)) .. " ")
    end
    return "|" .. table.concat(parts, "|") .. "|"
  end

  table.insert(lines, separator())
  table.insert(lines, row_line(columns))
  table.insert(lines, separator())
  local first_data_line = #lines + 1
  for _, row in ipairs(rows) do
    table.insert(lines, row_line(row))
  end
  table.insert(lines, separator())
  table.insert(lines, "(" .. row_count .. " row" .. (row_count == 1 and "" or "s") .. ")")

  return lines, { widths = widths, first_data_line = first_data_line }
end

function M.set_buffer_result(buf, result)
  vim.b[buf].sqlazo_result = result
  vim.api.nvim_buf_set_option(buf, "modifiable", true)
  local lines, meta = M.render_lines(result)
  vim.b[buf].sqlazo_result_meta = meta
  vim.b[buf].sqlazo_selection = { row = 1, col = 1 }
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
  vim.api.nvim_buf_set_option(buf, "filetype", "sqlazo-result")
  vim.api.nvim_buf_set_option(buf, "modifiable", false)
  M.highlight_selection(buf)
end

function M.set_buffer_error(buf, message)
  vim.b[buf].sqlazo_result = nil
  vim.b[buf].sqlazo_result_meta = nil
  vim.b[buf].sqlazo_selection = nil
  vim.api.nvim_buf_set_option(buf, "modifiable", true)
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, vim.split(message, "\n", { trimempty = false }))
  vim.api.nvim_buf_set_option(buf, "filetype", "sqlazo-result")
  vim.api.nvim_buf_set_option(buf, "modifiable", false)
end

return M
