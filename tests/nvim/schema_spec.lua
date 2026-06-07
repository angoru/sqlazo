-- Tests for the autocomplete logic (nvim/lua/sqlazo/schema.lua).

local t = require("harness")
local internal = require("sqlazo.schema")._internal

local schema = {
  tables = { "users", "orders" },
  columns = {
    users = {
      { name = "id", type = "INTEGER", key = "PRI" },
      { name = "name", type = "TEXT", key = "" },
      { name = "email", type = "TEXT", key = "UNI" },
    },
    orders = {
      { name = "id", type = "INTEGER", key = "PRI" },
      { name = "user_id", type = "INTEGER", key = "MUL" },
    },
  },
}

t.test("completion_context: table after FROM/JOIN", function()
  t.eq(internal.completion_context("SELECT * FROM "), "table")
  t.eq(internal.completion_context("SELECT * FROM users u JOIN "), "table")
end)

t.test("completion_context: column after WHERE/AND/ON/GROUP BY/HAVING/ORDER BY", function()
  t.eq(internal.completion_context("SELECT * FROM users WHERE "), "column")
  t.eq(internal.completion_context("SELECT * FROM users WHERE id = 1 AND "), "column")
  t.eq(internal.completion_context("SELECT * FROM a JOIN b ON "), "column")
  t.eq(internal.completion_context("SELECT * FROM users GROUP BY "), "column")
  t.eq(internal.completion_context("SELECT * FROM users GROUP BY id HAVING "), "column")
  t.eq(internal.completion_context("SELECT * FROM users ORDER BY "), "column")
end)

t.test("completion_context: column in SELECT list before FROM", function()
  t.eq(internal.completion_context("SELECT "), "column")
  t.eq(internal.completion_context("SELECT id, na"), "column")
end)

t.test("completion_context: qualified after alias dot", function()
  t.eq(internal.completion_context("SELECT * FROM users u WHERE u."), "qualified")
end)

t.test("completion_context: only the statement after the last ; counts", function()
  t.eq(internal.completion_context("SELECT 1; SELECT * FROM "), "table")
end)

t.test("completion_context: none for unrelated text", function()
  t.eq(internal.completion_context("-- just a comment FOO BAR"), "none")
end)

t.test("column_items: key columns sort first and show PRI/UNI", function()
  local items = internal.column_items(schema, "SELECT * FROM users")
  t.eq(#items, 3)

  local by_name = {}
  for _, item in ipairs(items) do
    by_name[item.label] = item
  end

  t.ok(by_name.id, "expected an 'id' item")
  t.ok(by_name.id.sortText:match("^0_"), "PRI column should sort first")
  t.ok(by_name.id.detail:find("PRI", 1, true), "PRI column detail should mention PRI")

  t.ok(by_name.email.sortText:match("^0_"), "UNI column should sort first")
  t.ok(by_name.email.detail:find("UNI", 1, true), "UNI column detail should mention UNI")

  t.ok(by_name.name.sortText:match("^1_"), "non-key column should sort after keys")
  t.ok(not by_name.name.detail:find("PRI", 1, true), "non-key detail should not mention PRI")
end)

t.test("column_items: resolves alias to its table", function()
  local items = internal.column_items(schema, "SELECT * FROM orders o", "orders")
  local labels = {}
  for _, item in ipairs(items) do
    labels[item.label] = true
  end
  t.ok(labels.user_id, "expected orders.user_id")
  t.ok(labels.id, "expected orders.id")
  t.ok(not labels.email, "should not include users columns")
end)

t.test("table_items: lists tables with table kind", function()
  local items = internal.table_items(schema)
  t.eq(#items, 2)
  t.eq(items[1].label, "users")
  t.eq(items[1].kind, 7)
end)

t.test("referenced_tables and table_aliases", function()
  local stmt = "SELECT * FROM users u JOIN orders AS o ON o.user_id = u.id"
  t.eq(internal.referenced_tables(stmt), { "USERS", "ORDERS" })
  local aliases = internal.table_aliases(stmt)
  t.eq(aliases.u, "USERS")
  t.eq(aliases.o, "ORDERS")
end)
