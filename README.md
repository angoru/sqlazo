# sqlazo

Execute SQL queries from files with connection headers.

## Components

This project contains two components:

| Component | Description |
|-----------|-------------|
| [cli/](./cli/) | Python CLI tool - `pip install -e ./cli` |
| [nvim/](./nvim/) | Neovim plugin - add to runtimepath |

## Quick Start

### CLI

```bash
cd cli && pip install -e .
sqlazo query.sql
```

### Neovim (lazy.nvim)

```lua
{
  dir = "/path/to/sqlazo/nvim",
  ft = "sql",
  config = function()
    require("sqlazo").setup()
  end,
}
```

## Supported Databases

- MySQL (`mysql://`)
- PostgreSQL (`postgresql://`)
- SQLite (`sqlite:///`)
- MongoDB (`mongodb://`)
- Redis (`redis://`)

## Examples

See [examples/](./examples/) for sample query files.

## License

MIT
