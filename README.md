# sqlazo

Execute SQL queries from files with connection headers.

## Components

This project contains two components:

| Component        | Description                              |
| ---------------- | ---------------------------------------- |
| [cli/](./cli/)   | Python CLI tool - `pip install -e ./cli` |
| [nvim/](./nvim/) | Neovim plugin - add to runtimepath       |

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
