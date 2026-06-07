-- Tiny zero-dependency test harness for headless Neovim (`nvim -l`).

local M = {
  passed = 0,
  failed = 0,
  failures = {},
}

local function fail(name, message)
  M.failed = M.failed + 1
  table.insert(M.failures, name .. ": " .. message)
  print(string.format("  not ok - %s\n      %s", name, message))
end

function M.test(name, fn)
  local ok, err = pcall(fn)
  if ok then
    M.passed = M.passed + 1
    print("  ok - " .. name)
  else
    fail(name, tostring(err))
  end
end

local function dump(value)
  if type(value) == "table" then
    return vim.inspect(value)
  end
  return tostring(value)
end

function M.eq(actual, expected, message)
  if not vim.deep_equal(actual, expected) then
    error(
      (message or "values differ")
        .. "\n      expected: "
        .. dump(expected)
        .. "\n      actual:   "
        .. dump(actual),
      2
    )
  end
end

function M.ok(cond, message)
  if not cond then
    error(message or "expected truthy value", 2)
  end
end

function M.finish()
  print(string.format("\n%d passed, %d failed", M.passed, M.failed))
  if M.failed > 0 then
    os.exit(1)
  end
  os.exit(0)
end

return M
