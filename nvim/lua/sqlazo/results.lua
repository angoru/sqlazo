-- sqlazo.nvim result rendering

local M = {}

local ns = vim.api.nvim_create_namespace("sqlazo_result")

local function stringify(value)
  if value == vim.NIL or value == nil then
    return "NULL"
  end
  return tostring(value)
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
  if not result or not meta then
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
  local win = vim.api.nvim_get_current_win()
  if vim.api.nvim_win_get_buf(win) == buf then
    pcall(vim.api.nvim_win_set_cursor, win, { line, start_col })
  end
end

function M.move_selection(buf, drow, dcol)
  local selection = clamp_selection(buf)
  if not selection then
    vim.api.nvim_echo({ { "sqlazo: No selectable result cells", "WarningMsg" } }, true, {})
    return
  end

  selection.row = selection.row + drow
  selection.col = selection.col + dcol
  vim.b[buf].sqlazo_selection = selection
  M.highlight_selection(buf)
end

function M.selected_cell(buf)
  local result = vim.b[buf].sqlazo_result
  local selection = clamp_selection(buf)
  if not result or not selection then
    return nil
  end

  return {
    column = result.columns[selection.col],
    value = result.rows[selection.row][selection.col],
    row = selection.row,
    col = selection.col,
  }
end

local function render_table(result)
  local columns = result.columns or {}
  local rows = result.rows or {}
  local lines = {}

  if #columns == 0 then
    return { "(No results)" }, nil
  end

  local widths = {}
  for i, column in ipairs(columns) do
    widths[i] = #stringify(column)
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
  table.insert(lines, "(" .. #rows .. " row" .. (#rows == 1 and "" or "s") .. ")")

  return lines, { widths = widths, first_data_line = first_data_line }
end

function M.set_result(buf, result)
  vim.b[buf].sqlazo_result = result
  vim.b[buf].sqlazo_selection = { row = 1, col = 1 }

  local lines, meta
  if result.raw_output then
    lines = vim.split(result.raw_output, "\n", { trimempty = false })
  elseif result.is_select == false then
    lines = { "Affected rows: " .. tostring(result.affected_rows or 0) }
  else
    lines, meta = render_table(result)
  end

  vim.b[buf].sqlazo_result_meta = meta
  vim.api.nvim_buf_set_option(buf, "modifiable", true)
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, lines)
  vim.api.nvim_buf_set_option(buf, "filetype", "sqlazo-result")
  vim.api.nvim_buf_set_option(buf, "modifiable", false)
  M.highlight_selection(buf)
end

function M.set_error(buf, message)
  vim.b[buf].sqlazo_result = nil
  vim.b[buf].sqlazo_result_meta = nil
  vim.b[buf].sqlazo_selection = nil
  vim.api.nvim_buf_set_option(buf, "modifiable", true)
  vim.api.nvim_buf_set_lines(buf, 0, -1, false, vim.split(message, "\n", { trimempty = false }))
  vim.api.nvim_buf_set_option(buf, "filetype", "sqlazo-result")
  vim.api.nvim_buf_set_option(buf, "modifiable", false)
end

function M.setup_keymaps(buf)
  vim.keymap.set("n", "h", function() M.move_selection(buf, 0, -1) end, { buffer = buf, desc = "Move cell left" })
  vim.keymap.set("n", "j", function() M.move_selection(buf, 1, 0) end, { buffer = buf, desc = "Move cell down" })
  vim.keymap.set("n", "k", function() M.move_selection(buf, -1, 0) end, { buffer = buf, desc = "Move cell up" })
  vim.keymap.set("n", "l", function() M.move_selection(buf, 0, 1) end, { buffer = buf, desc = "Move cell right" })
  vim.keymap.set("n", "f", function() require("sqlazo.runner").filter_by_selected_value(buf) end, {
    buffer = buf,
    desc = "Filter source query by selected cell",
  })
end

return M
