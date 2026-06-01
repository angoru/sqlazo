# sqlazo.nvim

Minimal Neovim plugin for running the SQL query at the cursor through the
`sqlazo` CLI.

## Requirements

- Neovim 0.8+
- `sqlazo` CLI in `PATH`, or configure `python_cmd`

## Installation

```lua
{
  dir = "/path/to/sqlazo/nvim",
  ft = { "sql", "mysql", "pgsql", "psql", "sqlite" },
  config = function()
    require("sqlazo").setup()
  end,
}
```

## Command

| Command | Description |
|---------|-------------|
| `:SqlazoRun` | Execute query at cursor |
| `:SqlazoFilterValue` | Add a filter to the source query using the selected result cell |

## Result Navigation

| Key | Action |
|-----|--------|
| `h`/`j`/`k`/`l` | Move selected result cell |
| `f` | Filter source query by selected cell |

## Autocomplete

```lua
require("cmp").setup({
  sources = {
    { name = "sqlazo", keyword_length = 0, priority = 1000 },
  },
})

require("sqlazo").setup_cmp()
```

The minimal autocomplete source suggests tables after `FROM`/`JOIN` and fields
from referenced tables in `WHERE`, `AND`, `OR`, `ON`, and `ORDER BY`.

## Configuration

```lua
require("sqlazo").setup({
  format = "table",
  python_cmd = "python",
  prefer_python = false,
  safe_mode = true,
  profile = nil,
})
```
