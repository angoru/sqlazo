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
| `:SqlazoRunTab` | Execute → tab |
| `:SqlazoRunInline [N]` | Insert first N rows as comments |
| `:SqlazoRunAllInline [N]` | Run all queries, update inline |
| `:SqlazoConsole` | Open interactive SQL console |

## Result Buffer

| Key | Action |
|-----|--------|
| `h`/`j`/`k`/`l` | Move selected cell |
| `yc` | Copy selected cell |
| `yr` | Copy selected row |
| `yC` | Copy selected column |
| `e` | Export result to CSV |
| `r` | Re-run query |
| `gq` / `<BS>` | Jump back to query |
| `g?` | Show contextual help |

## Configuration

```lua
require("sqlazo").setup({
  format = "table",      -- table, csv, json, record
  result_mode = "panel", -- panel, split, float, tab
  result_position = "bottom",
  reuse_result_buffer = true,
  python_cmd = "python", -- used for python -m sqlazo
  prefer_python = false, -- set true to ignore an old sqlazo in PATH
  auto_prefer_json_meta = true,
  safe_mode = true,      -- Confirm destructive queries
  default_comment_prefix = "--",
  comment_prefix_by_filetype = {
    sql = "--",
    javascript = "//",
    redis = "#",
  },
})
```

## Autocomplete

```lua
require("cmp").setup({
  sources = {
    { name = "sqlazo", keyword_length = 0, priority = 1000 },
  },
})

require("sqlazo").setup_cmp()
```

## License

MIT
