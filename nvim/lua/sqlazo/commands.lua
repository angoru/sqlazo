-- sqlazo.nvim commands module
-- Vim command definitions

local M = {}

local runner = require("sqlazo.runner")
local console = require("sqlazo.console")
local config = require("sqlazo.config")
local schema = require("sqlazo.schema")

local function sqlazo_cmd(...)
  local cmd = runner.get_cmd()
  for _, arg in ipairs({ ... }) do
    table.insert(cmd, arg)
  end
  return cmd
end

-- Helper function to parse arguments that might include a profile
local function parse_args_with_profile(args_str)
  if not args_str or args_str == "" then
    return nil, config.get().profile
  end

  local args = vim.split(args_str, " ", { trimempty = true })
  local max_rows = tonumber(args[1])
  local profile = args[2] or config.get().profile

  -- If first arg is not a number, treat it as profile
  if not max_rows then
    profile = args[1] or config.get().profile
    max_rows = 5
  end

  return max_rows, profile
end

-- Register all user commands
function M.setup()
  vim.api.nvim_create_user_command("SqlazoRun", function(opts)
    local max_rows, profile = parse_args_with_profile(opts.args)
    runner.run({
      format = config.get().format,
      profile = profile
    })
  end, { nargs = "*", desc = "Execute current query and show results" })

  vim.api.nvim_create_user_command("SqlazoRunAll", function(opts)
    local max_rows, profile = parse_args_with_profile(opts.args)
    runner.run({
      query_mode = "all",
      profile = profile
    })
  end, { nargs = "*", desc = "Execute all queries in buffer" })

  vim.api.nvim_create_user_command("SqlazoRunVertical", function(opts)
    local max_rows, profile = parse_args_with_profile(opts.args)
    runner.run({
      result_mode = "split",
      split = "vertical",
      profile = profile
    })
  end, { nargs = "*", desc = "Execute current query in vertical split" })

  vim.api.nvim_create_user_command("SqlazoRunHorizontal", function(opts)
    local max_rows, profile = parse_args_with_profile(opts.args)
    runner.run({
      result_mode = "split",
      split = "horizontal",
      profile = profile
    })
  end, { nargs = "*", desc = "Execute current query in horizontal split" })

  vim.api.nvim_create_user_command("SqlazoRunFloat", function(opts)
    local max_rows, profile = parse_args_with_profile(opts.args)
    runner.run({
      result_mode = "float",
      profile = profile
    })
  end, { nargs = "*", desc = "Execute current query in floating window" })

  vim.api.nvim_create_user_command("SqlazoRunTab", function(opts)
    local max_rows, profile = parse_args_with_profile(opts.args)
    runner.run({
      result_mode = "tab",
      profile = profile
    })
  end, { nargs = "*", desc = "Execute current query in a tab" })

  vim.api.nvim_create_user_command("SqlazoRunRecord", function(opts)
    local max_rows, profile = parse_args_with_profile(opts.args)
    runner.run({
      format = "record",
      profile = profile
    })
  end, { nargs = "*", desc = "Execute current query with record format" })

  vim.api.nvim_create_user_command("SqlazoRunInline", function(opts)
    local max_rows, profile = parse_args_with_profile(opts.args)
    runner.run_inline(max_rows, { profile = profile })
  end, { nargs = "*", desc = "Insert first N rows below query as comments" })

  vim.api.nvim_create_user_command("SqlazoResult", function()
    runner.focus_last_result()
  end, { desc = "Jump to last sqlazo result for current buffer" })

  vim.api.nvim_create_user_command("SqlazoRunAllInline", function(opts)
    local max_rows, profile = parse_args_with_profile(opts.args)
    runner.run_all_inline(max_rows, { profile = profile })
  end, { nargs = "*", desc = "Run all queries and update inline results" })

  vim.api.nvim_create_user_command("SqlazoConsole", function()
    console.open()
  end, { desc = "Open interactive SQL console" })

  vim.api.nvim_create_user_command("SqlazoVersion", function()
    local sqlazo = require("sqlazo")
    vim.api.nvim_echo({{"sqlazo.nvim v" .. (sqlazo.version or "unknown"), "Normal"}}, true, {})
  end, { desc = "Show sqlazo version" })

  vim.api.nvim_create_user_command("SqlazoDebug", function()
    runner.reset_detection_cache()
    local cfg = config.get()
    local runner_source = debug.getinfo(runner.get_cmd, "S").source
    local lines = {
      "sqlazo debug:",
      "  runner: " .. runner_source,
      "  python_cmd: " .. tostring(cfg.python_cmd),
      "  prefer_python: " .. tostring(cfg.prefer_python),
      "  auto_prefer_json_meta: " .. tostring(cfg.auto_prefer_json_meta),
      "  cmd: " .. table.concat(runner.get_cmd(), " "),
      "  json_meta: " .. tostring(runner.supports_json_meta()),
    }
    vim.api.nvim_echo({{table.concat(lines, "\n"), "Normal"}}, true, {})
  end, { desc = "Show sqlazo debug info" })

  vim.api.nvim_create_user_command("SqlazoSchema", function()
    local current_schema, err = schema.get(true)
    if err then
      vim.api.nvim_echo({{"sqlazo schema error: " .. err, "ErrorMsg"}}, true, {})
      return
    end

    local tables = current_schema.tables or {}
    local preview_tables = {}
    for i = 1, math.min(#tables, 20) do
      table.insert(preview_tables, tables[i])
    end
    local preview = table.concat(preview_tables, ", ")
    if #tables > 20 then
      preview = preview .. ", ..."
    end
    vim.api.nvim_echo({{"sqlazo schema: " .. #tables .. " tables" .. (#preview > 0 and " (" .. preview .. ")" or ""), "Normal"}}, true, {})
  end, { desc = "Fetch and show autocomplete schema tables" })

  vim.api.nvim_create_user_command("SqlazoHelp", function()
    local lines = {
      "sqlazo.nvim commands:",
      "  :SqlazoRun [N] [profile] - Execute current query",
      "  :SqlazoRunAll [N] [profile] - Execute all queries",
      "  :SqlazoRunVertical [N] [profile] - Execute in vertical split",
      "  :SqlazoRunHorizontal [N] [profile] - Execute in horizontal split",
      "  :SqlazoRunFloat [N] [profile] - Execute in floating window",
      "  :SqlazoRunTab [N] [profile] - Execute in tab",
      "  :SqlazoRunRecord [N] [profile] - Execute with record format",
      "  :SqlazoRunInline [N] [profile] - Insert results as comments",
      "  :SqlazoRunAllInline [N] [profile] - Update all inline results",
      "  :SqlazoResult - Jump to last result",
      "  :SqlazoConsole - Open SQL console",
      "  :SqlazoVersion - Show version",
      "  :SqlazoDebug - Show debug info",
      "  :SqlazoSchema - Show autocomplete schema tables",
    }
    vim.api.nvim_echo({{table.concat(lines, "\n"), "Normal"}}, true, {})
  end, { desc = "Show sqlazo commands" })

  -- Credential management commands
  vim.api.nvim_create_user_command("SqlazoStoreCreds", function(opts)
    local args = vim.split(opts.args, " ", { trimempty = true })
    if #args < 2 then
      vim.api.nvim_echo({{"Usage: SqlazoStoreCreds <profile> <host> [port] [user] [password] [database] [db_type]", "ErrorMsg"}}, true, {})
      return
    end

    local profile = args[1]
    local host = args[2]
    local port = args[3] ~= "" and tonumber(args[3]) or nil
    local user = args[4] ~= "" and args[4] or nil
    local password = args[5] ~= "" and args[5] or nil
    local database = args[6] ~= "" and args[6] or nil
    local db_type = args[7] ~= "" and args[7] or nil

    local cmd_parts = sqlazo_cmd("cred", "store", profile)
    local env = nil
    if host then table.insert(cmd_parts, "--host"); table.insert(cmd_parts, host) end
    if port then table.insert(cmd_parts, "--port"); table.insert(cmd_parts, tostring(port)) end
    if user then table.insert(cmd_parts, "--user"); table.insert(cmd_parts, user) end
    if password then
      table.insert(cmd_parts, "--password-env")
      table.insert(cmd_parts, "SQLAZO_DB_PASSWORD")
      env = { SQLAZO_DB_PASSWORD = password }
    end
    if database then table.insert(cmd_parts, "--database"); table.insert(cmd_parts, database) end
    if db_type then table.insert(cmd_parts, "--db-type"); table.insert(cmd_parts, db_type) end

    vim.fn.jobstart(cmd_parts, {
      env = env,
      on_exit = function(_, code, _)
        if code == 0 then
          vim.api.nvim_echo({{"Credentials stored successfully", "Normal"}}, true, {})
        else
          vim.api.nvim_echo({{"Failed to store credentials", "ErrorMsg"}}, true, {})
        end
      end
    })
  end, { nargs = "*", desc = "Store encrypted credentials" })

  vim.api.nvim_create_user_command("SqlazoListCreds", function()
    local output = vim.fn.system(sqlazo_cmd("cred", "list"))
    if vim.v.shell_error == 0 then
      vim.api.nvim_echo({{"Stored credential profiles:", "Normal"}}, false, {})
      local lines = vim.split(output, "\n")
      for _, line in ipairs(lines) do
        if line ~= "" then
          vim.api.nvim_echo({{line, "Normal"}}, true, {})
        end
      end
    else
      vim.api.nvim_echo({{"Error listing credentials: " .. output, "ErrorMsg"}}, true, {})
    end
  end, { desc = "List stored credential profiles" })

  vim.api.nvim_create_user_command("SqlazoRetrieveCreds", function(opts)
    local profile = opts.args
    if not profile or profile == "" then
      vim.api.nvim_echo({{"Usage: SqlazoRetrieveCreds <profile>", "ErrorMsg"}}, true, {})
      return
    end

    local output = vim.fn.system(sqlazo_cmd("cred", "retrieve", profile))
    if vim.v.shell_error == 0 then
      vim.api.nvim_echo({{"Credential profile '" .. profile .. "':", "Normal"}}, false, {})
      local lines = vim.split(output, "\n")
      for _, line in ipairs(lines) do
        if line ~= "" then
          vim.api.nvim_echo({{line, "Normal"}}, true, {})
        end
      end
    else
      vim.api.nvim_echo({{"Error retrieving credentials: " .. output, "ErrorMsg"}}, true, {})
    end
  end, { nargs = 1, desc = "Retrieve stored credentials" })
end

return M
