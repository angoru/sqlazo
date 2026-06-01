# sqlazo

Execute SQL queries from files with connection headers.

`sqlazo` has two parts:

- a Python CLI that reads connection settings from SQL file headers or environment variables
- a small Neovim plugin that runs the query at the cursor and works with the result buffer

## Components

| Component        | Description                              |
| ---------------- | ---------------------------------------- |
| [cli/](./cli/)   | Python CLI tool - `pip install -e ./cli` |
| [nvim/](./nvim/) | Neovim plugin - add to runtimepath       |

## Quick Start

### CLI

```bash
cd cli && pip install -e .
sqlazo query path/to/query.sql
```

SQL files can include connection settings in header comments:

```sql
-- dbtype: mariadb
-- host: db.example.local
-- port: 3306
-- user: app_user
-- database: app_db

SELECT * FROM users LIMIT 10;
```

You can also read the query from stdin:

```bash
sqlazo query -f table -
```

Available output formats:

- `table`
- `csv`
- `json`
- `record`
- `json-meta`

### Neovim (lazy.nvim)

```lua
{
  dir = "/path/to/sqlazo/nvim",
  ft = { "sql", "mysql", "pgsql", "psql", "sqlite" },
  config = function()
    require("sqlazo").setup({
      python_cmd = "/path/to/python",
      prefer_python = true,
    })
  end,
}
```

Run the SQL query at the cursor:

```vim
:SqlazoRun
```

The result buffer supports:

| Key | Action |
|-----|--------|
| `h`/`j`/`k`/`l` | Move selected result cell |
| `f` | Filter the source query by the selected cell and rerun |
| `y` | Yank the selected cell |

Filtering edits the source query physically. Each filter is one undo step, so
`u` walks back filters one by one.

If a search is active in the result buffer, `f` filters the selected column with
`LIKE '%search%'`. Without an active search, it filters by exact cell value.

Optional autocomplete through `nvim-cmp`:

```lua
require("cmp").setup({
  sources = {
    { name = "sqlazo", keyword_length = 0, priority = 1000 },
  },
})

require("sqlazo").setup_cmp()
```

Autocomplete suggests tables after `FROM`/`JOIN` and fields from referenced
tables in `WHERE`, `AND`, `OR`, `ON`, and `ORDER BY`.

## Supported Databases

- MySQL (`mysql://`)
- PostgreSQL (`postgresql://`)
- SQLite (`sqlite:///`)

## Examples

See [examples/](./examples/) for sample query files and [nvim/README.md](./nvim/README.md)
for plugin-specific details.

## Versioning

This project follows [Semantic Versioning](https://semver.org/).

### Updating

If you installed via `pipx` (recommended):

```bash
# Update to the latest version
pipx upgrade sqlazo

# If installed from local source and source changed
pipx reinstall sqlazo
```

## License

MIT

---

![Made with AI](https://img.shields.io/badge/Made%20with-AI-blue)
