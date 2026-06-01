-- sqlazo.nvim parser module
-- Handles header and query parsing from buffers

local M = {}

local config = require("sqlazo.config")

local function unique_prefixes()
  local prefixes = {}
  local map = config.get().comment_prefix_by_filetype or {}
  for _, prefix in pairs(map) do
    prefixes[prefix] = true
  end
  prefixes[config.get().default_comment_prefix or "--"] = true

  local list = {}
  for prefix, _ in pairs(prefixes) do
    table.insert(list, prefix)
  end
  return list
end

function M.get_comment_prefixes()
  return unique_prefixes()
end

local function matches_header_prefix(trimmed, prefix)
  if prefix == "#" then
    return trimmed:match("^#[^!].*:")
  end
  local escaped = vim.pesc(prefix)
  return trimmed:match("^" .. escaped .. ".*:")
end

local function matches_comment_prefix(trimmed, prefix)
  if prefix == "#" then
    return trimmed:match("^#") and not trimmed:match("^#!")
  end
  local escaped = vim.pesc(prefix)
  return trimmed:match("^" .. escaped)
end

-- Check if a line is a header comment (-- key: value, // key: value, or # key: value)
function M.is_header_line(line, prefixes)
  local trimmed = line:match("^%s*(.-)%s*$")
  local prefix_list = prefixes or M.get_comment_prefixes()
  for _, prefix in ipairs(prefix_list) do
    if matches_header_prefix(trimmed, prefix) then
      return true
    end
  end
  return false
end

function M.is_comment_line(line, prefixes)
  local trimmed = line:match("^%s*(.-)%s*$")
  local prefix_list = prefixes or M.get_comment_prefixes()
  for _, prefix in ipairs(prefix_list) do
    if matches_comment_prefix(trimmed, prefix) then
      return true
    end
  end
  return false
end

-- Extract header (connection info) from buffer lines
function M.get_header(lines)
  local prefixes = M.get_comment_prefixes()
  local header_lines = {}
  for _, line in ipairs(lines) do
    local trimmed = line:match("^%s*(.-)%s*$")
    if M.is_header_line(line, prefixes) then
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
  local prefixes = M.get_comment_prefixes()

  -- Find start of current query (search backwards for empty line or header)
  for i = cursor_line, 1, -1 do
    local line = lines[i]
    local trimmed = line:match("^%s*(.-)%s*$")

    if trimmed == "" then
      query_start = i + 1
      break
    elseif M.is_header_line(line, prefixes) then
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
    if not M.is_header_line(line, prefixes) then
      table.insert(query_lines, line)
    end
  end

  local has_sql = false
  for _, line in ipairs(query_lines) do
    local trimmed = line:match("^%s*(.-)%s*$")
    if trimmed ~= "" and not M.is_comment_line(line, prefixes) then
      has_sql = true
      break
    end
  end

  if not has_sql then
    query_lines = {}
  end

  return query_lines, query_start, query_end
end

-- Check if a line is an inline result comment
function M.is_inline_result(line, prefixes)
  local trimmed = line:match("^%s*(.-)%s*$")
  local prefix_list = prefixes
  if type(prefixes) == "string" then
    prefix_list = { prefixes }
  elseif not prefixes then
    prefix_list = M.get_comment_prefixes()
  end

  for _, prefix in ipairs(prefix_list) do
    local escaped = vim.pesc(prefix)
    local base = "^" .. escaped .. "%s*"
    if trimmed:match(base .. "%+%-") or
       trimmed:match(base .. "|") or
       trimmed:match(base .. "%(%d+ rows?%)") or
       trimmed:match(base .. "Affected rows:") or
       trimmed:match(base .. "ERROR:") or
       trimmed == prefix or
       trimmed == prefix .. " " then
      return true
    end
  end
  return false
end

-- Find all queries in buffer (returns list of {start, end})
function M.find_all_queries(all_lines)
  local queries = {}
  local header_end = 0
  local prefixes = M.get_comment_prefixes()

  -- Find where header ends
  for i, line in ipairs(all_lines) do
    local trimmed = line:match("^%s*(.-)%s*$")
    if M.is_header_line(line, prefixes) then
      header_end = i
    elseif trimmed ~= "" and not M.is_comment_line(line, prefixes) then
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

    if M.is_inline_result(line, prefixes) then
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

    if M.is_header_line(line, prefixes) then
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

function M.get_comment_prefix(lines, filetype)
  local prefixes = M.get_comment_prefixes()
  local header_lines = M.get_header(lines or {})
  for _, line in ipairs(header_lines) do
    local trimmed = line:match("^%s*(.-)%s*$")
    for _, prefix in ipairs(prefixes) do
      if matches_header_prefix(trimmed, prefix) then
        return prefix
      end
    end
  end

  local ft = filetype or vim.bo.filetype
  local map = config.get().comment_prefix_by_filetype or {}
  return map[ft] or config.get().default_comment_prefix or "--"
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
