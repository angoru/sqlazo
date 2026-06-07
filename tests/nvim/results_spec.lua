-- Tests for the result table renderer (nvim/lua/sqlazo/results.lua).

local t = require("harness")
local results = require("sqlazo.results")

local function render(result)
  local buf = vim.api.nvim_create_buf(false, true)
  results.set_result(buf, result)
  local lines = vim.api.nvim_buf_get_lines(buf, 0, -1, false)
  return buf, lines
end

-- Every border (+...) and data (|...) line must have the same screen width,
-- otherwise the table is misaligned.
local function assert_aligned(lines)
  local width
  for _, line in ipairs(lines) do
    if line:match("^[+|]") then
      local w = vim.fn.strdisplaywidth(line)
      if width == nil then
        width = w
      elseif w ~= width then
        error(
          string.format(
            "misaligned line (width %d, expected %d):\n      %q",
            w,
            width,
            line
          ),
          2
        )
      end
    end
  end
  t.ok(width ~= nil, "expected at least one rendered table line")
end

t.test("aligns multibyte UTF-8 cells", function()
  local _, lines = render({
    is_select = true,
    columns = { "code", "name" },
    rows = {
      { "REG-039", "Tripulación mínima" },
      { "REG-007", "Combate a distancia" },
      { "REG-DRAFT-001", "Daño, heridas y fuera de combate" },
      { "REG-029", "Disparo de Cañones" },
    },
  })
  assert_aligned(lines)
end)

t.test("aligns double-width (CJK) cells", function()
  local _, lines = render({
    is_select = true,
    columns = { "id", "label" },
    rows = {
      { "1", "東京" },
      { "2", "ok" },
    },
  })
  assert_aligned(lines)
end)

t.test("renders columns wider than 99 chars without error", function()
  local wide = string.rep("x", 150)
  local ok, lines = pcall(function()
    local _, l = render({
      is_select = true,
      columns = { "blob" },
      rows = { { wide } },
    })
    return l
  end)
  t.ok(ok, "rendering a 150-char cell should not error: " .. tostring(lines))
  assert_aligned(lines)
  local found = false
  for _, line in ipairs(lines) do
    if line:find(wide, 1, true) then
      found = true
    end
  end
  t.ok(found, "the wide value should appear in the output")
end)

t.test("flattens newlines so buffer lines never contain \\n", function()
  local ok, lines = pcall(function()
    local _, l = render({
      is_select = true,
      columns = { "note" },
      rows = { { "line one\nline two\r\nline three" } },
    })
    return l
  end)
  t.ok(ok, "multiline cells should not error: " .. tostring(lines))
  for _, line in ipairs(lines) do
    t.ok(not line:find("\n", 1, true), "no buffer line may contain a newline")
    t.ok(not line:find("\r", 1, true), "no buffer line may contain a carriage return")
  end
  assert_aligned(lines)
end)

t.test("renders NULL for nil/vim.NIL values", function()
  local _, lines = render({
    is_select = true,
    columns = { "a", "b" },
    rows = { { vim.NIL, "x" } },
  })
  local joined = table.concat(lines, "\n")
  t.ok(joined:find("NULL", 1, true), "nil values should render as NULL")
  assert_aligned(lines)
end)

t.test("non-select results show affected rows", function()
  local _, lines = render({ is_select = false, affected_rows = 3 })
  t.eq(lines, { "Affected rows: 3" })
end)
