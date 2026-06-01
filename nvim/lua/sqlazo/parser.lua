-- sqlazo.nvim parser

local M = {}

local config = require("sqlazo.config")

local function unique_prefixes()
  local seen = {}
  local prefixes = {}
  local map = config.get().comment_prefix_by_filetype or {}

  for _, prefix in pairs(map) do
    if not seen[prefix] then
      seen[prefix] = true
      table.insert(prefixes, prefix)
    end
  end

  local default = config.get().default_comment_prefix or "--"
  if not seen[default] then
    table.insert(prefixes, default)
  end

  return prefixes
end

local function trim(line)
  return (line or ""):match("^%s*(.-)%s*$")
end

local function starts_with_prefix(line, prefix)
  if prefix == "#" then
    return line:match("^#") and not line:match("^#!")
  end
  return line:match("^" .. vim.pesc(prefix)) ~= nil
end

function M.is_header_line(line)
  local stripped = trim(line)
  for _, prefix in ipairs(unique_prefixes()) do
    if starts_with_prefix(stripped, prefix) and stripped:match(":%s*.+$") then
      return true
    end
  end
  return false
end

local function is_comment_line(line)
  local stripped = trim(line)
  for _, prefix in ipairs(unique_prefixes()) do
    if starts_with_prefix(stripped, prefix) then
      return true
    end
  end
  return false
end

function M.get_header(lines)
  local header = {}
  for _, line in ipairs(lines or {}) do
    local stripped = trim(line)
    if M.is_header_line(line) then
      table.insert(header, line)
    elseif stripped == "" then
      break
    else
      break
    end
  end
  return header
end

local function has_sql(lines)
  for _, line in ipairs(lines) do
    local stripped = trim(line)
    if stripped ~= "" and not is_comment_line(line) then
      return true
    end
  end
  return false
end

function M.get_query_at_cursor(lines)
  local cursor_line = vim.fn.line(".")
  local query_start = 1
  local query_end = #lines

  for i = cursor_line, 1, -1 do
    local stripped = trim(lines[i])
    if stripped == "" or M.is_header_line(lines[i]) then
      query_start = i + 1
      break
    end
  end

  for i = cursor_line, #lines do
    local stripped = trim(lines[i])
    if stripped == "" and i > cursor_line then
      query_end = i - 1
      break
    end
    if stripped:match(";%s*$") then
      query_end = i
      break
    end
  end

  local query = {}
  for i = query_start, query_end do
    if not M.is_header_line(lines[i]) then
      table.insert(query, lines[i])
    end
  end

  if not has_sql(query) then
    return {}, query_start, query_end
  end

  return query, query_start, query_end
end

function M.build_content(header_lines, query_lines)
  local content = {}
  for _, line in ipairs(header_lines) do
    table.insert(content, line)
  end
  table.insert(content, "")
  for _, line in ipairs(query_lines) do
    table.insert(content, line)
  end
  return table.concat(content, "\n")
end

return M
