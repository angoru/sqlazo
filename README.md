# sqlazo

Run SQL queries from Neovim through a single `sqlazo` binary.

`sqlazo` has two parts:

- a Rust CLI binary that reads connection settings from SQL file headers, environment variables, or `.env`
- a Neovim plugin that runs the query at the cursor and renders a navigable result buffer

## Requirements

- Rust toolchain for building the binary
- Neovim 0.8+
- `sqlazo` binary in `PATH`, or configure `sqlazo_cmd`

## Build

```bash
cargo build --release
```

The binary is written to:

```bash
target/release/sqlazo
```

## Prebuilt Binaries

GitHub Actions builds `sqlazo` binaries on every push to `main`, pull request,
manual workflow run, and version tag matching `v*`.

For normal `main` builds, download the generated files from the workflow run
artifacts in the GitHub Actions page. For version tags, the same archives are
attached automatically to the GitHub release.

Current release targets:

| Platform | Artifact |
|----------|----------|
| Linux x86_64 GNU | `sqlazo-<version>-linux-x86_64-gnu.tar.gz` |
| Linux x86_64 musl | `sqlazo-<version>-linux-x86_64-musl.tar.gz` |
| Linux ARM64 GNU | `sqlazo-<version>-linux-aarch64-gnu.tar.gz` |
| Linux ARM64 musl | `sqlazo-<version>-linux-aarch64-musl.tar.gz` |
| Linux ARMv7 GNU hard-float | `sqlazo-<version>-linux-armv7-gnueabihf.tar.gz` |
| macOS x86_64 | `sqlazo-<version>-macos-x86_64.tar.gz` |
| macOS Apple Silicon | `sqlazo-<version>-macos-aarch64.tar.gz` |
| Windows x86_64 MSVC | `sqlazo-<version>-windows-x86_64-msvc.zip` |
| Windows ARM64 MSVC | `sqlazo-<version>-windows-aarch64-msvc.zip` |

Each archive contains the `sqlazo` binary, plus `README.md` and `LICENSE` when
present.

## CLI Surface

The Rust CLI intentionally implements the process API used by the Neovim plugin:

```bash
sqlazo query -f json-meta -
sqlazo query --schema -
```

Both commands read SQL content from stdin. SQL files can include connection
settings in header comments:

```sql
-- dbtype: mariadb
-- host: db.example.local
-- port: 3306
-- user: app_user
-- database: app_db

SELECT * FROM users LIMIT 10;
```

You can also use connection URLs:

```sql
-- url: sqlite:///./app.db

SELECT * FROM users;
```

Connection priority is:

1. SQL header
2. `DB_*` environment variables
3. `.env`
4. defaults

Supported environment variables:

- `DB_TYPE`
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_DATABASE`

## Supported Databases

- SQLite (`sqlite:///`)
- PostgreSQL (`postgresql://`, `postgres://`)
- MySQL (`mysql://`)
- MariaDB (`mariadb://`)

## Neovim

```lua
{
  dir = "/path/to/sqlazo/nvim",
  ft = { "sql", "mysql", "pgsql", "psql", "sqlite" },
  config = function()
    require("sqlazo").setup({
      sqlazo_cmd = "sqlazo",
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
