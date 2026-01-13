-- sqlazo.nvim parser module
-- Handles header and query parsing from buffers

local M = {}

local config = require("sqlazo.config")

-- Comment prefixes for different file types
M.comment_prefixes = {
  sql = "--",
  javascript = "//",
  redis = "#",
}

-- Check if a line is a header comment (-- key: value, // key: value, or # key: value)
function M.is_header_line(line)
  local trimmed = line:match("^%s*(.-)%s*$")
  -- SQL-style: -- key: value
  if trimmed:match("^%-%-.*:") then
    return true
  end
  -- JavaScript-style: // key: value (for MongoDB)
  if trimmed:match("^//.*:") then
    return true
  end
  -- Hash-style: # key: value (for Redis)
  if trimmed:match("^#[^!].*:") then -- exclude shebang
    return true
  end
  return false
end

-- Extract header (connection info) from buffer lines
function M.get_header(lines)
  local header_lines = {}
  for _, line in ipairs(lines) do
    local trimmed = line:match("^%s*(.-)%s*$")
    if M.is_header_line(line) then
      table.insert(header_lines, line)
    elseif trimmed == "" then
      break
    else
      break
    end
  end
  return header_lines
end

-- Find the query under cursor
-- Returns: query_lines, query_start, query_end
function M.get_query_at_cursor(lines)
  local cursor_line = vim.fn.line(".")
  local query_start = nil
  local query_end = nil

  -- Find start of current query (search backwards for empty line or header)
  for i = cursor_line, 1, -1 do
    local line = lines[i]
    local trimmed = line:match("^%s*(.-)%s*$")

    if trimmed == "" then
      query_start = i + 1
      break
    elseif M.is_header_line(line) then
      query_start = i + 1
      break
    end
  end
  query_start = query_start or 1

  -- Find end of current query (search forwards for empty line or semicolon at end)
  for i = cursor_line, #lines do
    local line = lines[i]
    local trimmed = line:match("^%s*(.-)%s*$")

    if trimmed == "" and i > cursor_line then
      query_end = i - 1
      break
    elseif trimmed:match(";%s*$") then
      query_end = i
      break
    end
  end
  query_end = query_end or #lines

  -- Extract query lines
  local query_lines = {}
  for i = query_start, query_end do
    local line = lines[i]
    if not M.is_header_line(line) then
      table.insert(query_lines, line)
    end
  end

  return query_lines, query_start, query_end
end

-- Check if a line is an inline result comment
function M.is_inline_result(line)
  local trimmed = line:match("^%s*(.-)%s*$")
  return trimmed:match("^%-%-%s*%+%-") or
         trimmed:match("^%-%-%s*|") or
         trimmed:match("^%-%-%s*%(%d+ rows?%)") or
         trimmed:match("^%-%-%s*Affected rows:") or
         trimmed:match("^%-%-%s*ERROR:") or
         trimmed == "--" or
         trimmed == "-- "
end

-- Find all queries in buffer (returns list of {start, end})
function M.find_all_queries(all_lines)
  local queries = {}
  local header_end = 0

  -- Find where header ends
  for i, line in ipairs(all_lines) do
    local trimmed = line:match("^%s*(.-)%s*$")
    if trimmed:match("^%-%-.*:") or trimmed:match("^//.*:") or trimmed:match("^#[^!].*:") then
      header_end = i
    elseif trimmed ~= "" and not trimmed:match("^%-%-") and not trimmed:match("^//") and not trimmed:match("^#") then
      break
    elseif trimmed == "" and header_end > 0 then
      break
    end
  end

  local query_start = nil
  local in_query = false

  for i = header_end + 1, #all_lines do
    local line = all_lines[i]
    local trimmed = line:match("^%s*(.-)%s*$")

    if M.is_inline_result(line) then
      goto continue
    end

    if trimmed == "" then
      if in_query then
        table.insert(queries, { start = query_start, finish = i - 1 })
        in_query = false
        query_start = nil
      end
      goto continue
    end

    if trimmed:match("^%-%-.*:") or trimmed:match("^//.*:") or trimmed:match("^#[^!].*:") then
      goto continue
    end

    if not in_query then
      query_start = i
      in_query = true
    end

    if trimmed:match(";%s*$") then
      table.insert(queries, { start = query_start, finish = i })
      in_query = false
      query_start = nil
    end

    ::continue::
  end

  if in_query and query_start then
    table.insert(queries, { start = query_start, finish = #all_lines })
  end

  return queries
end

-- Build content from header and query lines
function M.build_content(header_lines, query_lines)
  local content_lines = {}
  for _, line in ipairs(header_lines) do
    table.insert(content_lines, line)
  end
  table.insert(content_lines, "")
  for _, line in ipairs(query_lines) do
    table.insert(content_lines, line)
  end
  return table.concat(content_lines, "\n")
end

return M
