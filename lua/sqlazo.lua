-- sqlazo.nvim - Execute SQL queries from Neovim
-- Lua plugin for sqlazo CLI tool

local M = {}

-- Default configuration
M.config = {
  format = "table",      -- Output format: table, csv, json
  split = "float",       -- Split direction: horizontal, vertical, float
  python_cmd = "python", -- Python command (python, python3, etc.)
  safe_mode = true,      -- Confirm before executing destructive queries (INSERT/UPDATE/DELETE/DROP/etc.)
}

-- Destructive SQL keywords that modify database
M.destructive_keywords = {
  "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", 
  "CREATE", "REPLACE", "RENAME", "GRANT", "REVOKE"
}

-- Check if query contains destructive operations
function M.is_destructive_query(query)
  local upper_query = query:upper()
  for _, keyword in ipairs(M.destructive_keywords) do
    -- Match keyword at start of query or after whitespace/newline
    if upper_query:match("^%s*" .. keyword .. "%s") or 
       upper_query:match("\n%s*" .. keyword .. "%s") then
      return true, keyword
    end
  end
  return false, nil
end

-- Confirm destructive query execution
function M.confirm_destructive(keyword, callback)
  vim.ui.select(
    {"No, cancel", "Yes, execute"},
    {
      prompt = "⚠️  Query contains " .. keyword .. ". Execute anyway?",
    },
    function(choice)
      if choice == "Yes, execute" then
        callback()
      else
        vim.api.nvim_echo({{"sqlazo: Query cancelled", "WarningMsg"}}, true, {})
      end
    end
  )
end

-- Setup function to configure the plugin
function M.setup(opts)
  M.config = vim.tbl_deep_extend("force", M.config, opts or {})
  
  -- Create user commands
  vim.api.nvim_create_user_command("SqlazoRun", function()
    M.run()
  end, { desc = "Execute current query and show results" })
  
  vim.api.nvim_create_user_command("SqlazoRunAll", function()
    M.run({ query_mode = "all" })
  end, { desc = "Execute all queries in buffer" })
  
  vim.api.nvim_create_user_command("SqlazoRunVertical", function()
    M.run({ split = "vertical" })
  end, { desc = "Execute current query in vertical split" })
  
  vim.api.nvim_create_user_command("SqlazoRunHorizontal", function()
    M.run({ split = "horizontal" })
  end, { desc = "Execute current query in horizontal split" })
  
  vim.api.nvim_create_user_command("SqlazoRunFloat", function()
    M.run({ split = "float" })
  end, { desc = "Execute current query in floating window" })
  
  vim.api.nvim_create_user_command("SqlazoRunRecord", function()
    M.run({ format = "record" })
  end, { desc = "Execute current query with record format (one field per line)" })
  
  vim.api.nvim_create_user_command("SqlazoRunInline", function(opts)
    local max_rows = 5
    if opts.args and opts.args ~= "" then
      max_rows = tonumber(opts.args) or 5
    end
    M.run_inline(max_rows)
  end, { nargs = "?", desc = "Insert first N rows (default 5) below query as comments" })
  
  vim.api.nvim_create_user_command("SqlazoRunAllInline", function(opts)
    local max_rows = 5
    if opts.args and opts.args ~= "" then
      max_rows = tonumber(opts.args) or 5
    end
    M.run_all_inline(max_rows)
  end, { nargs = "?", desc = "Run all queries and update inline results" })
  
  vim.api.nvim_create_user_command("SqlazoConsole", function()
    M.open_console()
  end, { desc = "Open interactive SQL console" })
end

-- Get the sqlazo command
function M.get_cmd()
  -- Try to find sqlazo in PATH first, otherwise use python -m
  local handle = io.popen("which sqlazo 2>/dev/null")
  if handle then
    local result = handle:read("*a")
    handle:close()
    if result and result:match("%S") then
      return "sqlazo"
    end
  end
  return M.config.python_cmd .. " -m sqlazo"
end

-- Check if a line is a header comment (-- key: value or // key: value)
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
  return false
end

-- Extract header (connection info) from buffer
function M.get_header(lines)
  local header_lines = {}
  for _, line in ipairs(lines) do
    local trimmed = line:match("^%s*(.-)%s*$")
    if M.is_header_line(line) then
      -- This is a header line (-- key: value or // key: value)
      table.insert(header_lines, line)
    elseif trimmed == "" then
      -- Empty line, might end header
      break
    else
      -- Non-header content, stop
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
      -- Header line, query starts after
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
    -- Skip comment-only lines that look like headers
    if not M.is_header_line(line) then
      table.insert(query_lines, line)
    end
  end
  
  return query_lines, query_start, query_end
end

-- Execute query and insert first N rows as comments below
function M.run_inline(max_rows)
  max_rows = max_rows or 5
  
  -- Get buffer lines
  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  
  -- Get header
  local header_lines = M.get_header(all_lines)
  
  -- Get query at cursor with positions
  local query_lines, query_start, query_end = M.get_query_at_cursor(all_lines)
  
  -- Combine header + query
  local content_lines = {}
  for _, line in ipairs(header_lines) do
    table.insert(content_lines, line)
  end
  table.insert(content_lines, "")  -- Empty line between header and query
  for _, line in ipairs(query_lines) do
    table.insert(content_lines, line)
  end
  
  local content = table.concat(content_lines, "\n")
  
  -- Build command
  local cmd = M.get_cmd() .. " -f table -"
  
  -- Execute command with stdin
  local output = vim.fn.system(cmd, content)
  local exit_code = vim.v.shell_error
  
  if exit_code ~= 0 then
    vim.api.nvim_echo({{"sqlazo: Query failed - " .. output, "ErrorMsg"}}, true, {})
    return
  end
  
  -- Parse output and take first N rows + header
  local result_lines = vim.split(output, "\n", { trimempty = false })
  local inline_lines = {}
  local row_count = 0
  local header_found = false
  
  for i, line in ipairs(result_lines) do
    -- Skip empty lines at start
    if line:match("^%s*$") and #inline_lines == 0 then
      goto continue
    end
    
    -- Check if this is the row count line - add it and stop
    if line:match("^%(%d+ rows?%)$") or line:match("^Affected rows:") then
      table.insert(inline_lines, "-- " .. line)
      break
    end
    
    -- Count data rows (lines with | but not separator lines)
    if line:match("^|.*|$") and not line:match("^%+%-") then
      if header_found then
        row_count = row_count + 1
      else
        header_found = true  -- First | row is header
      end
    end
    
    table.insert(inline_lines, "-- " .. line)
    
    -- Stop after max_rows data rows (plus header and separators)
    if row_count >= max_rows then
      -- Add closing separator if next line is one
      if result_lines[i + 1] and result_lines[i + 1]:match("^%+%-") then
        table.insert(inline_lines, "-- " .. result_lines[i + 1])
      end
      -- Add row count from end
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
  
  -- Insert results below the query
  table.insert(inline_lines, 1, "")  -- Empty line before results
  vim.api.nvim_buf_set_lines(0, query_end, query_end, false, inline_lines)
  
  vim.api.nvim_echo({{"sqlazo: Inserted " .. row_count .. " rows inline", "Normal"}}, true, {})
end

-- Check if a line is an inline result comment
function M.is_inline_result(line)
  local trimmed = line:match("^%s*(.-)%s*$")
  -- Match lines like: -- +---+---+ or -- | val | or -- (N rows) or -- Affected rows:
  return trimmed:match("^%-%-%s*%+%-") or 
         trimmed:match("^%-%-%s*|") or 
         trimmed:match("^%-%-%s*%(%d+ rows?%)") or
         trimmed:match("^%-%-%s*Affected rows:") or
         trimmed:match("^%-%-%s*ERROR:") or
         trimmed == "--" or
         trimmed == "-- "
end

-- Find all queries in buffer (returns list of {start, end, lines})
function M.find_all_queries(all_lines)
  local queries = {}
  local header_end = 0
  
  -- Find where header ends
  for i, line in ipairs(all_lines) do
    local trimmed = line:match("^%s*(.-)%s*$")
    if trimmed:match("^%-%-.*:") then
      header_end = i
    elseif trimmed ~= "" and not trimmed:match("^%-%-") then
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
    
    -- Skip inline results
    if M.is_inline_result(line) then
      goto continue
    end
    
    -- Skip empty lines between queries
    if trimmed == "" then
      if in_query then
        -- End current query
        table.insert(queries, { start = query_start, finish = i - 1 })
        in_query = false
        query_start = nil
      end
      goto continue
    end
    
    -- Skip header-style comments
    if trimmed:match("^%-%-.*:") then
      goto continue
    end
    
    -- This is query content
    if not in_query then
      query_start = i
      in_query = true
    end
    
    -- Check for query end (semicolon)
    if trimmed:match(";%s*$") then
      table.insert(queries, { start = query_start, finish = i })
      in_query = false
      query_start = nil
    end
    
    ::continue::
  end
  
  -- Handle query without trailing semicolon
  if in_query and query_start then
    table.insert(queries, { start = query_start, finish = #all_lines })
  end
  
  return queries
end

-- Execute all queries and update inline results
function M.run_all_inline(max_rows)
  max_rows = max_rows or 5
  
  -- Get buffer lines
  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  
  -- Get header
  local header_lines = M.get_header(all_lines)
  local header_content = table.concat(header_lines, "\n")
  
  -- First pass: remove all existing inline results
  local cleaned_lines = {}
  for _, line in ipairs(all_lines) do
    if not M.is_inline_result(line) then
      table.insert(cleaned_lines, line)
    end
  end
  
  -- Find all queries in cleaned buffer
  local queries = M.find_all_queries(cleaned_lines)
  
  if #queries == 0 then
    vim.api.nvim_echo({{"sqlazo: No queries found", "WarningMsg"}}, true, {})
    return
  end
  
  -- Process queries in reverse order (so line numbers stay valid)
  local results = {}
  for _, q in ipairs(queries) do
    -- Extract query lines
    local query_text = {}
    for i = q.start, q.finish do
      table.insert(query_text, cleaned_lines[i])
    end
    
    -- Build content with header + query
    local content = header_content .. "\n\n" .. table.concat(query_text, "\n")
    
    -- Execute
    local cmd = M.get_cmd() .. " -f table -"
    local output = vim.fn.system(cmd, content)
    local exit_code = vim.v.shell_error
    
    if exit_code ~= 0 then
      table.insert(results, { pos = q.finish, lines = {"", "-- ERROR: " .. output:gsub("\n", " ")} })
    else
      -- Parse and format result
      local result_lines = vim.split(output, "\n", { trimempty = false })
      local inline_lines = {}
      local row_count = 0
      local header_found = false
      
      for i, line in ipairs(result_lines) do
        if line:match("^%s*$") and #inline_lines == 0 then
          goto skip
        end
        
        -- Check if this is the row count line - add it and stop
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
          -- Add row count from end
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
  
  -- Apply results in reverse order
  for i = #results, 1, -1 do
    local r = results[i]
    -- Find actual position in cleaned_lines
    local insert_pos = r.pos
    -- Insert after the query
    for j, line in ipairs(r.lines) do
      table.insert(cleaned_lines, insert_pos + j, line)
    end
  end
  
  -- Replace buffer content
  vim.api.nvim_buf_set_lines(0, 0, -1, false, cleaned_lines)
  
  vim.api.nvim_echo({{"sqlazo: Updated " .. #queries .. " queries inline", "Normal"}}, true, {})
end

-- Execute the current buffer as a SQL file
function M.run(opts)
  opts = vim.tbl_deep_extend("force", {}, M.config, opts or {})
  opts.query_mode = opts.query_mode or "cursor"  -- "cursor" or "all"
  
  -- Get buffer lines
  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  
  -- Get header
  local header_lines = M.get_header(all_lines)
  
  -- Get query based on mode
  local query_lines
  if opts.query_mode == "all" then
    -- Use all content after header
    query_lines = {}
    local in_header = true
    for _, line in ipairs(all_lines) do
      local trimmed = line:match("^%s*(.-)%s*$")
      if in_header then
        if not trimmed:match("^%-%-.*:") and trimmed ~= "" then
          in_header = false
          table.insert(query_lines, line)
        end
      else
        table.insert(query_lines, line)
      end
    end
  else
    -- Get query at cursor
    query_lines = M.get_query_at_cursor(all_lines)
  end
  
  -- Combine header + query
  local content_lines = {}
  for _, line in ipairs(header_lines) do
    table.insert(content_lines, line)
  end
  table.insert(content_lines, "")  -- Empty line between header and query
  for _, line in ipairs(query_lines) do
    table.insert(content_lines, line)
  end
  
  local content = table.concat(content_lines, "\n")
  
  -- Actual execution function
  local function do_run()
    -- Build command
    local cmd = M.get_cmd() .. " -f " .. opts.format .. " -"
    
    -- Execute command with stdin
    local output = vim.fn.system(cmd, content)
    local exit_code = vim.v.shell_error
    
    -- Create result buffer
    local buf = vim.api.nvim_create_buf(false, true)  -- unlisted, scratch
    vim.api.nvim_buf_set_option(buf, "buftype", "nofile")
    vim.api.nvim_buf_set_option(buf, "bufhidden", "wipe")
    vim.api.nvim_buf_set_option(buf, "swapfile", false)
    
    -- Set buffer name (with timestamp to avoid duplicates)
    local source_name = vim.fn.expand("%:t")
    if source_name == "" then source_name = "[Scratch]" end
    local timestamp = os.date("%H%M%S")
    pcall(vim.api.nvim_buf_set_name, buf, "sqlazo://" .. source_name .. "." .. timestamp)
    
    -- Set content
    local result_lines = vim.split(output, "\n", { trimempty = false })
    vim.api.nvim_buf_set_lines(buf, 0, -1, false, result_lines)
    
    -- Set buffer as readonly
    vim.api.nvim_buf_set_option(buf, "modifiable", false)
    
    -- Open in appropriate window
    if opts.split == "float" then
      M.open_float(buf)
    elseif opts.split == "vertical" then
      vim.cmd("vsplit")
      vim.api.nvim_win_set_buf(0, buf)
    else
      vim.cmd("split")
      vim.api.nvim_win_set_buf(0, buf)
    end
    
    -- Set filetype for syntax highlighting
    if opts.format == "json" then
      vim.api.nvim_buf_set_option(buf, "filetype", "json")
    elseif opts.format == "csv" then
      vim.api.nvim_buf_set_option(buf, "filetype", "csv")
    end
    
    -- Show error status if command failed
    if exit_code ~= 0 then
      vim.api.nvim_echo({{"sqlazo: Query failed (see output)", "ErrorMsg"}}, true, {})
    end
  end
  
  -- Check for destructive query if safe_mode is enabled
  if M.config.safe_mode then
    local is_destructive, keyword = M.is_destructive_query(content)
    if is_destructive then
      M.confirm_destructive(keyword, do_run)
      return
    end
  end
  
  -- Execute directly if not destructive or safe_mode is off
  do_run()
end

-- Open buffer in floating window
function M.open_float(buf)
  local width = math.floor(vim.o.columns * 0.8)
  local height = math.floor(vim.o.lines * 0.8)
  local row = math.floor((vim.o.lines - height) / 2)
  local col = math.floor((vim.o.columns - width) / 2)
  
  local win_opts = {
    relative = "editor",
    width = width,
    height = height,
    row = row,
    col = col,
    style = "minimal",
    border = "rounded",
    title = " sqlazo results ",
    title_pos = "center",
  }
  
  local win = vim.api.nvim_open_win(buf, true, win_opts)
  
  -- Close on q or Escape
  vim.api.nvim_buf_set_keymap(buf, "n", "q", ":close<CR>", { noremap = true, silent = true })
  vim.api.nvim_buf_set_keymap(buf, "n", "<Esc>", ":close<CR>", { noremap = true, silent = true })
end

-- Interactive SQL Console
-- State for console
M.console = {
  query_buf = nil,
  result_buf = nil,
  query_win = nil,
  result_win = nil,
  source_buf = nil,      -- Original buffer where query came from
  source_query_start = nil,  -- Start line of query in source buffer
  source_query_end = nil,    -- End line of query in source buffer
  source_header_end = nil,   -- End line of header in source buffer
}

-- Open interactive SQL console
function M.open_console()
  local width = math.floor(vim.o.columns * 0.8)
  local height = math.floor(vim.o.lines * 0.8)
  local row = math.floor((vim.o.lines - height) / 2)
  local col = math.floor((vim.o.columns - width) / 2)
  
  -- Split height: 40% query, 60% results
  local query_height = math.floor(height * 0.4)
  local result_height = height - query_height - 1  -- -1 for gap
  
  -- Create query buffer (editable)
  local query_buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_buf_set_option(query_buf, "buftype", "nofile")
  vim.api.nvim_buf_set_option(query_buf, "bufhidden", "wipe")
  vim.api.nvim_buf_set_option(query_buf, "filetype", "sql")
  
  -- Store source buffer for save functionality
  local source_buf = vim.api.nvim_get_current_buf()
  
  -- Get connection header and current query from current buffer
  local current_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = M.get_header(current_lines)
  local query_lines, query_start, query_end = M.get_query_at_cursor(current_lines)
  
  -- Build initial content: header + query
  local initial_lines = {}
  for _, line in ipairs(header_lines) do
    table.insert(initial_lines, line)
  end
  if #header_lines > 0 then
    table.insert(initial_lines, "")
  end
  for _, line in ipairs(query_lines) do
    table.insert(initial_lines, line)
  end
  
  if #initial_lines > 0 then
    vim.api.nvim_buf_set_lines(query_buf, 0, -1, false, initial_lines)
  end
  
  -- Create result buffer (readonly)
  local result_buf = vim.api.nvim_create_buf(false, true)
  vim.api.nvim_buf_set_option(result_buf, "buftype", "nofile")
  vim.api.nvim_buf_set_option(result_buf, "bufhidden", "wipe")
  vim.api.nvim_buf_set_option(result_buf, "modifiable", false)
  
  -- Open query window (top)
  local query_win = vim.api.nvim_open_win(query_buf, true, {
    relative = "editor",
    width = width,
    height = query_height,
    row = row,
    col = col,
    style = "minimal",
    border = "rounded",
    title = " SQL Query (F5/Enter: exec, Ctrl+S: save, q: close) ",
    title_pos = "center",
  })
  
  -- Open result window (bottom)
  local result_win = vim.api.nvim_open_win(result_buf, false, {
    relative = "editor",
    width = width,
    height = result_height,
    row = row + query_height + 2,  -- +2 for border
    col = col,
    style = "minimal",
    border = "rounded",
    title = " Results ",
    title_pos = "center",
  })
  
  -- Store state
  M.console.query_buf = query_buf
  M.console.result_buf = result_buf
  M.console.query_win = query_win
  M.console.result_win = result_win
  M.console.source_buf = source_buf
  M.console.source_query_start = query_start
  M.console.source_query_end = query_end
  M.console.source_header_end = #header_lines
  
  -- Function to execute query
  local function execute_console_query()
    local lines = vim.api.nvim_buf_get_lines(query_buf, 0, -1, false)
    local content = table.concat(lines, "\n")
    
    -- Actual execution function
    local function do_execute()
      local cmd = M.get_cmd() .. " -f " .. M.config.format .. " -"
      local output = vim.fn.system(cmd, content)
      
      -- Update result buffer
      vim.api.nvim_buf_set_option(result_buf, "modifiable", true)
      local result_lines = vim.split(output, "\n", { trimempty = false })
      vim.api.nvim_buf_set_lines(result_buf, 0, -1, false, result_lines)
      vim.api.nvim_buf_set_option(result_buf, "modifiable", false)
      
      -- Show status
      if vim.v.shell_error ~= 0 then
        vim.api.nvim_echo({{"sqlazo: Query failed", "ErrorMsg"}}, true, {})
      else
        vim.api.nvim_echo({{"sqlazo: Query executed", "Normal"}}, true, {})
      end
    end
    
    -- Check for destructive query if safe_mode is enabled
    if M.config.safe_mode then
      local is_destructive, keyword = M.is_destructive_query(content)
      if is_destructive then
        M.confirm_destructive(keyword, do_execute)
        return
      end
    end
    
    -- Execute directly if not destructive or safe_mode is off
    do_execute()
  end
  
  -- Function to close console
  local function close_console()
    pcall(vim.api.nvim_win_close, result_win, true)
    pcall(vim.api.nvim_win_close, query_win, true)
    M.console = {}
  end
  
  -- Function to save query back to source buffer
  local function save_query_to_source()
    -- Check if source buffer still exists
    if not vim.api.nvim_buf_is_valid(source_buf) then
      vim.api.nvim_echo({{"sqlazo: Source buffer no longer exists", "ErrorMsg"}}, true, {})
      return
    end
    
    -- Get current console query content (excluding header)
    local console_lines = vim.api.nvim_buf_get_lines(query_buf, 0, -1, false)
    local console_header = M.get_header(console_lines)
    
    -- Extract only the query part (skip header and empty line after it)
    local query_only = {}
    local skip_count = #console_header
    if #console_header > 0 then
      skip_count = skip_count + 1  -- Skip empty line after header
    end
    for i = skip_count + 1, #console_lines do
      table.insert(query_only, console_lines[i])
    end
    
    -- Replace the original query lines in source buffer
    vim.api.nvim_buf_set_lines(source_buf, query_start - 1, query_end, false, query_only)
    
    -- Update stored positions for subsequent saves
    M.console.source_query_end = query_start - 1 + #query_only
    
    -- Get source buffer name for feedback
    local source_name = vim.api.nvim_buf_get_name(source_buf)
    if source_name == "" then source_name = "[Scratch]" 
    else source_name = vim.fn.fnamemodify(source_name, ":t")
    end
    
    vim.api.nvim_echo({{"sqlazo: Query saved to " .. source_name, "Normal"}}, true, {})
  end
  
  -- Keymaps for query buffer
  -- Execute: Ctrl+x, F5, or <leader>r in normal mode
  vim.keymap.set({"n", "i"}, "<C-x>", execute_console_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set({"n", "i"}, "<F5>", execute_console_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set("n", "<leader>r", execute_console_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set("n", "<CR>", execute_console_query, { buffer = query_buf, desc = "Execute query" })
  vim.keymap.set("n", "q", close_console, { buffer = query_buf, desc = "Close console" })
  vim.keymap.set("n", "<Esc>", close_console, { buffer = query_buf, desc = "Close console" })
  vim.keymap.set({"n", "i"}, "<C-s>", save_query_to_source, { buffer = query_buf, desc = "Save query to source" })
  
  -- Keymaps for result buffer
  vim.keymap.set("n", "q", close_console, { buffer = result_buf, desc = "Close console" })
  vim.keymap.set("n", "<Esc>", close_console, { buffer = result_buf, desc = "Close console" })
  vim.keymap.set("n", "<Tab>", function()
    vim.api.nvim_set_current_win(query_win)
  end, { buffer = result_buf, desc = "Switch to query" })
  
  -- Tab to switch between windows
  vim.keymap.set("n", "<Tab>", function()
    vim.api.nvim_set_current_win(result_win)
  end, { buffer = query_buf, desc = "Switch to results" })
  
  -- Start in insert mode at end of buffer
  vim.cmd("startinsert!")
end

-- ============================================================================
-- Schema Introspection & Autocomplete
-- ============================================================================

-- Schema cache keyed by connection URL
M.schema_cache = {}

-- Get connection URL from buffer header for cache key
function M.get_connection_key(lines)
  lines = lines or vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = M.get_header(lines)
  local header = table.concat(header_lines, "\n")
  
  -- Use hash of header as cache key
  local key = ""
  for _, line in ipairs(header_lines) do
    local trimmed = line:match("^%s*(.-)%s*$")
    if trimmed:match("^%-%-.*:") then
      key = key .. trimmed
    end
  end
  return key
end

-- Fetch schema from database using CLI
function M.get_schema(force_refresh)
  local all_lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
  local header_lines = M.get_header(all_lines)
  
  if #header_lines == 0 then
    return nil, "No connection header found in buffer"
  end
  
  local cache_key = M.get_connection_key(all_lines)
  
  -- Return cached schema if available
  if not force_refresh and M.schema_cache[cache_key] then
    return M.schema_cache[cache_key]
  end
  
  -- Build content with just header (no query needed for schema)
  local content = table.concat(header_lines, "\n") .. "\n\nSELECT 1;"
  
  -- Execute sqlazo --schema
  local cmd = M.get_cmd() .. " --schema -"
  local output = vim.fn.system(cmd, content)
  local exit_code = vim.v.shell_error
  
  if exit_code ~= 0 then
    return nil, "Failed to fetch schema: " .. output
  end
  
  -- Parse JSON response
  local ok, schema = pcall(vim.json.decode, output)
  if not ok then
    return nil, "Failed to parse schema JSON: " .. tostring(schema)
  end
  
  -- Cache the result
  M.schema_cache[cache_key] = schema
  
  return schema
end

-- Clear schema cache (useful after schema changes)
function M.clear_schema_cache()
  M.schema_cache = {}
  vim.api.nvim_echo({{"sqlazo: Schema cache cleared", "Normal"}}, true, {})
end

-- Extract tables mentioned in query (FROM, JOIN, INTO, UPDATE)
function M.extract_tables_from_query(query_text)
  local tables = {}
  local upper_query = query_text:upper()
  local lower_query = query_text:lower()
  
  -- Patterns to find table names after FROM, JOIN, INTO, UPDATE
  -- Match: FROM table, JOIN table, INTO table, UPDATE table
  -- Also handles: FROM table AS alias, FROM table alias
  local patterns = {
    "FROM%s+([%w_]+)",
    "JOIN%s+([%w_]+)",
    "INTO%s+([%w_]+)",
    "UPDATE%s+([%w_]+)",
  }
  
  for _, pattern in ipairs(patterns) do
    for table_name in upper_query:gmatch(pattern) do
      -- Find original case from lower_query at same position
      local start_pos = upper_query:find(pattern)
      if start_pos then
        local actual_name = query_text:match("[%w_]+", upper_query:find(table_name, start_pos, true))
        if actual_name then
          tables[actual_name:lower()] = actual_name
        else
          tables[table_name:lower()] = table_name
        end
      else
        tables[table_name:lower()] = table_name
      end
    end
  end
  
  return tables
end

-- Determine SQL context based on cursor position
function M.get_sql_context(cursor_before, full_query)
  local upper_before = cursor_before:upper()
  
  -- Check what keyword we're closest to (search backwards)
  -- Priority: most recent keyword determines context
  
  -- After table prefix (e.g., "users.") - column completion
  if cursor_before:match("([%w_]+)%.$") then
    return "column_qualified"
  end
  
  -- Patterns for "expecting table name" contexts
  local table_contexts = {
    "FROM%s+$",
    "FROM%s+[%w_]+%s*,%s*$",
    "JOIN%s+$",
    "INTO%s+$",
    "UPDATE%s+$",
    "TABLE%s+$",
    "TRUNCATE%s+$",
    "DESC%s+$",
    "DESCRIBE%s+$",
  }
  
  for _, pattern in ipairs(table_contexts) do
    if upper_before:match(pattern) then
      return "table"
    end
  end
  
  -- Patterns for "expecting column name" contexts
  local column_contexts = {
    "SELECT%s+$",
    "SELECT%s+.+,%s*$",
    "WHERE%s+$",
    "WHERE%s+.+AND%s+$",
    "WHERE%s+.+OR%s+$",
    "SET%s+$",
    "SET%s+.+,%s*$",
    "ORDER%s+BY%s+$",
    "ORDER%s+BY%s+.+,%s*$",
    "GROUP%s+BY%s+$",
    "GROUP%s+BY%s+.+,%s*$",
    "HAVING%s+$",
    "ON%s+$",
    "AND%s+$",
    "OR%s+$",
  }
  
  for _, pattern in ipairs(column_contexts) do
    if upper_before:match(pattern) then
      return "column"
    end
  end
  
  -- Default: show both but prioritize tables
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
    local schema = M.get_schema()
    if not schema then
      callback({ items = {}, isIncomplete = false })
      return
    end
    
    local items = {}
    local cursor_before = params.context.cursor_before_line
    
    -- Get full query context (all lines up to cursor)
    local lines = vim.api.nvim_buf_get_lines(0, 0, -1, false)
    local header_lines = M.get_header(lines)
    local query_text = ""
    for i = #header_lines + 1, vim.fn.line(".") do
      if lines[i] then
        query_text = query_text .. lines[i] .. "\n"
      end
    end
    query_text = query_text .. cursor_before
    
    -- Determine context
    local context = M.get_sql_context(cursor_before, query_text)
    
    -- Check if we're after a table alias (e.g., "t." or "users.")
    local table_prefix = cursor_before:match("([%w_]+)%.$")
    
    if table_prefix or context == "column_qualified" then
      -- Show columns for specific table
      local table_name = table_prefix or cursor_before:match("([%w_]+)%.$")
      
      if schema.columns and table_name then
        -- Try exact match first
        if schema.columns[table_name] then
          for _, col in ipairs(schema.columns[table_name]) do
            table.insert(items, {
              label = col.name,
              kind = 5,  -- Field
              detail = col.type .. (col.key ~= "" and " [" .. col.key .. "]" or ""),
              documentation = "Column from " .. table_name,
            })
          end
        else
          -- Try case-insensitive match
          for tbl, cols in pairs(schema.columns) do
            if tbl:lower() == table_name:lower() then
              for _, col in ipairs(cols) do
                table.insert(items, {
                  label = col.name,
                  kind = 5,  -- Field
                  detail = col.type .. (col.key ~= "" and " [" .. col.key .. "]" or ""),
                  documentation = "Column from " .. tbl,
                })
              end
              break
            end
          end
        end
      end
      
    elseif context == "table" then
      -- Show only tables
      if schema.tables then
        for _, table_name in ipairs(schema.tables) do
          table.insert(items, {
            label = table_name,
            kind = 7,  -- Class (used for tables)
            detail = "Table",
            documentation = "Table: " .. table_name,
          })
        end
      end
      
    elseif context == "column" then
      -- Show columns from tables mentioned in query
      local mentioned_tables = M.extract_tables_from_query(query_text)
      local has_tables = next(mentioned_tables) ~= nil
      
      if has_tables and schema.columns then
        -- Show columns only from mentioned tables
        for tbl_lower, tbl_original in pairs(mentioned_tables) do
          -- Find matching table in schema (case-insensitive)
          for schema_tbl, cols in pairs(schema.columns) do
            if schema_tbl:lower() == tbl_lower then
              for _, col in ipairs(cols) do
                table.insert(items, {
                  label = col.name,
                  kind = 5,  -- Field
                  detail = col.type .. " (" .. schema_tbl .. ")",
                  documentation = "Column from " .. schema_tbl,
                  sortText = "a" .. col.name,  -- Sort first
                })
              end
              break
            end
          end
        end
        
        -- Also add table-qualified versions for clarity in JOINs
        if vim.tbl_count(mentioned_tables) > 1 then
          for tbl_lower, _ in pairs(mentioned_tables) do
            for schema_tbl, cols in pairs(schema.columns) do
              if schema_tbl:lower() == tbl_lower then
                for _, col in ipairs(cols) do
                  table.insert(items, {
                    label = schema_tbl .. "." .. col.name,
                    kind = 5,  -- Field
                    detail = col.type,
                    documentation = "Qualified column from " .. schema_tbl,
                    sortText = "b" .. schema_tbl .. col.name,
                  })
                end
                break
              end
            end
          end
        end
      else
        -- No tables mentioned yet, show all columns  
        if schema.columns then
          for tbl, cols in pairs(schema.columns) do
            for _, col in ipairs(cols) do
              table.insert(items, {
                label = col.name,
                kind = 5,  -- Field
                detail = col.type .. " (" .. tbl .. ")",
                filterText = col.name,
                sortText = "z" .. col.name,
              })
            end
          end
        end
      end
      
    else
      -- Mixed context: show tables first, then relevant columns
      if schema.tables then
        for _, table_name in ipairs(schema.tables) do
          table.insert(items, {
            label = table_name,
            kind = 7,  -- Class (used for tables)
            detail = "Table",
            documentation = "Table: " .. table_name,
            sortText = "a" .. table_name,
          })
        end
      end
      
      -- Add columns from mentioned tables only (if any)
      local mentioned_tables = M.extract_tables_from_query(query_text)
      if next(mentioned_tables) and schema.columns then
        for tbl_lower, _ in pairs(mentioned_tables) do
          for schema_tbl, cols in pairs(schema.columns) do
            if schema_tbl:lower() == tbl_lower then
              for _, col in ipairs(cols) do
                table.insert(items, {
                  label = col.name,
                  kind = 5,  -- Field
                  detail = col.type .. " (" .. schema_tbl .. ")",
                  sortText = "b" .. col.name,
                })
              end
              break
            end
          end
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

-- Register with nvim-cmp (call this in setup if cmp is available)
function M.setup_cmp()
  local ok, cmp = pcall(require, "cmp")
  if not ok then
    return false
  end
  
  cmp.register_source("sqlazo", M.get_cmp_source().new())
  return true
end

return M
