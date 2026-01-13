# sqlazo.nvim

Neovim plugin for executing SQL queries with database-aware autocomplete.

## Requirements

- Neovim 0.8+
- [sqlazo CLI](../cli/) installed and in PATH

## Installation

**lazy.nvim:**

```lua
{
  dir = "/path/to/sqlazo/nvim",
  ft = { "sql", "mysql", "javascript", "redis" },
  config = function()
    require("sqlazo").setup()
  end,
}
```

## Commands

| Command | Description |
|---------|-------------|
| `:SqlazoRun` | Execute query at cursor → float |
| `:SqlazoRunRecord` | Execute with record format |
| `:SqlazoRunVertical` | Execute → vertical split |
| `:SqlazoRunHorizontal` | Execute → horizontal split |
| `:SqlazoRunInline [N]` | Insert first N rows as comments |
| `:SqlazoRunAllInline [N]` | Run all queries, update inline |
| `:SqlazoConsole` | Open interactive SQL console |

## Configuration

```lua
require("sqlazo").setup({
  format = "table",      -- table, csv, json, record
  split = "float",       -- float, vertical, horizontal
  safe_mode = true,      -- Confirm destructive queries
})
```

## Autocomplete

```lua
require("cmp").setup({
  sources = {
    { name = "sqlazo" },
  },
})

require("sqlazo").setup_cmp()
```

## License

MIT
