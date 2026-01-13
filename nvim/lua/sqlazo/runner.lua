-- sqlazo.nvim runner module
-- Handles query execution via CLI

local M = {}

local config = require("sqlazo.config")
local parser = require("sqlazo.parser")
local ui = require("sqlazo.ui")

-- Get the sqlazo command
function M.get_cmd()
  -- Try to find sqlazo in PATH first, otherwise use python -m
  local handle = io.popen("which sqlazo 2>/dev/null || where sqlazo 2>NUL")
  if handle then
    local result = handle:read("*a")
    handle:close()
    if result and result:match("%S") then
      return "sqlazo"
    end
  end
  return config.get().python_cmd .. " -m sqlazo"
end

-- Execute a query and return output
function M.execute(content, format)
  format = format or config.get().format
  local cmd = M.get_cmd() .. " -f " .. format .. " -"
  local output = vim.fn.system(cmd, content)
  local exit_code = vim.v.shell_error
  return output, exit_code
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

  local content = parser.build_content(header_lines, query_lines)

  local function do_run()
    local output, exit_code = M.execute(content, opts.format)

    local source_name = vim.fn.expand("%:t")
    if source_name == "" then source_name = "[Scratch]" end
    local timestamp = os.date("%H%M%S")

    local buf = ui.create_scratch_buffer({
      name = "sqlazo://" .. source_name .. "." .. timestamp,
      filetype = opts.format == "json" and "json" or (opts.format == "csv" and "csv" or nil),
    })

    local result_lines = vim.split(output, "\n", { trimempty = false })
    vim.api.nvim_buf_set_lines(buf, 0, -1, false, result_lines)
    vim.api.nvim_buf_set_option(buf, "modifiable", false)

    if opts.split == "float" then
      ui.open_float(buf)
    else
      ui.open_split(buf, opts.split)
    end

    if exit_code ~= 0 then
      vim.api.nvim_echo({{"sqlazo: Query failed (see output)", "ErrorMsg"}}, true, {})
    end
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
function M.run_inline(max_rows)
  max_rows = max_rows or 5

  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = parser.get_header(all_lines)
  local query_lines, query_start, query_end = parser.get_query_at_cursor(all_lines)
  local content = parser.build_content(header_lines, query_lines)

  local output, exit_code = M.execute(content, "table")

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
      table.insert(inline_lines, "-- " .. line)
      break
    end

    if line:match("^|.*|$") and not line:match("^%+%-") then
      if header_found then
        row_count = row_count + 1
      else
        header_found = true
      end
    end

    table.insert(inline_lines, "-- " .. line)

    if row_count >= max_rows then
      if result_lines[i + 1] and result_lines[i + 1]:match("^%+%-") then
        table.insert(inline_lines, "-- " .. result_lines[i + 1])
      end
      for _, l in ipairs(result_lines) do
        if l:match("^%(%d+ rows?%)$") then
          table.insert(inline_lines, "-- " .. l)
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
function M.run_all_inline(max_rows)
  max_rows = max_rows or 5

  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = parser.get_header(all_lines)
  local header_content = table.concat(header_lines, "\n")

  -- Remove existing inline results
  local cleaned_lines = {}
  for _, line in ipairs(all_lines) do
    if not parser.is_inline_result(line) then
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
    local output, exit_code = M.execute(content, "table")

    if exit_code ~= 0 then
      table.insert(results, { pos = q.finish, lines = {"", "-- ERROR: " .. output:gsub("\n", " ")} })
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
          table.insert(inline_lines, "-- " .. line)
          break
        end

        if line:match("^|.*|$") and not line:match("^%+%-") then
          if header_found then
            row_count = row_count + 1
          else
            header_found = true
          end
        end

        table.insert(inline_lines, "-- " .. line)

        if row_count >= max_rows then
          if result_lines[i + 1] and result_lines[i + 1]:match("^%+%-") then
            table.insert(inline_lines, "-- " .. result_lines[i + 1])
          end
          for _, l in ipairs(result_lines) do
            if l:match("^%(%d+ rows?%)$") then
              table.insert(inline_lines, "-- " .. l)
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
