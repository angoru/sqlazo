# sqlazo

Execute SQL queries from files with connection headers — designed for Neovim.

## Installation

```bash
cd /path/to/sqlazo
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

**Neovim** (lazy.nvim):

```lua
{
  dir = "/path/to/sqlazo",
  ft = "sql",
  config = function()
    require("sqlazo").setup()
  end,
}
```

---

## Quick Start

```sql
-- url: mysql://user:pass@localhost:3306/mydb

SELECT * FROM users LIMIT 10;
```

Or with PostgreSQL:

```sql
-- url: postgresql://user:pass@localhost:5432/mydb

SELECT * FROM users LIMIT 10;
```

Or with SQLite:

```sql
-- url: sqlite:///./mydb.sqlite

SELECT * FROM users LIMIT 10;
```

```bash
sqlazo query.sql
```

---

## File Format

### URL Format (recommended)

**MySQL:**
```sql
-- url: mysql://user:password@localhost:3306/database

SELECT * FROM users;
```

**PostgreSQL:**
```sql
-- url: postgresql://user:password@localhost:5432/database

SELECT * FROM users;
```

**SQLite:**
```sql
-- url: sqlite:///path/to/database.db
-- or for in-memory: sqlite://:memory:

SELECT * FROM users;
```

### Key-Value Format

```sql
-- host: localhost
-- user: myuser
-- password: mypass
-- db: mydb

SELECT * FROM users;
```

---

## CLI Usage

```bash
sqlazo file.sql              # Table format (default)
sqlazo file.sql -f record    # One field per line (good for wide tables)
sqlazo file.sql -f json      # JSON output
sqlazo file.sql -f csv       # CSV output
sqlazo file.sql -v           # Verbose (show connection info)
cat file.sql | sqlazo -      # Read from stdin
```

---

## Neovim Commands

| Command                   | Description                                 |
| ------------------------- | ------------------------------------------- |
| `:SqlazoRun`              | Execute query at cursor → float             |
| `:SqlazoRunRecord`        | Execute with record format                  |
| `:SqlazoRunVertical`      | Execute → vertical split                    |
| `:SqlazoRunHorizontal`    | Execute → horizontal split                  |
| `:SqlazoRunInline [N]`    | Insert first N rows as comments below query |
| `:SqlazoRunAllInline [N]` | Run all queries, update inline results      |
| `:SqlazoConsole`          | Open interactive SQL console                |

---

## Interactive Console

Open with `:SqlazoConsole`:

```
┌─────────────────────────────────────┐
│  SQL Query (editable)               │
│  -- url: mysql://...                │
│  SELECT * FROM users;               │
└─────────────────────────────────────┘
┌─────────────────────────────────────┐
│  Results (readonly)                 │
│  +----+-------+                     │
│  | id | name  |                     │
└─────────────────────────────────────┘
```

**Keybindings:**
| Key | Action |
|-----|--------|
| `F5` or `Enter` | Execute query |
| `Ctrl+x` | Execute query |
| `Ctrl+s` | Save query to source buffer |
| `Tab` | Switch query/results |
| `q` or `Esc` | Close console |

---

## Safety Mode

By default, sqlazo asks for confirmation before executing destructive queries:

```
⚠️  Query contains DELETE. Execute anyway?
> No, cancel
  Yes, execute
```

**Detected keywords:** INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE, REPLACE, RENAME, GRANT, REVOKE

**Disable confirmation:**

```lua
require("sqlazo").setup({
  safe_mode = false,
})
```

---

## Configuration

```lua
require("sqlazo").setup({
  format = "table",      -- table, csv, json, record
  split = "float",       -- float, vertical, horizontal
  safe_mode = true,      -- Confirm destructive queries
})
```

### Environment Variables

| Variable          | Description         |
| ----------------- | ------------------- |
| `SQLAZO_HOST`     | Default host        |
| `SQLAZO_PORT`     | Default port (3306) |
| `SQLAZO_USER`     | Default user        |
| `SQLAZO_PASSWORD` | Default password    |
| `SQLAZO_DB`       | Default database    |

---

## Autocomplete

sqlazo provides database-aware autocomplete via `nvim-cmp`.

### Setup

```lua
-- In your nvim-cmp config, add sqlazo as a source:
require("cmp").setup({
  sources = {
    { name = "sqlazo" },  -- Add this
    -- ... other sources
  },
})

-- Register the source (call after sqlazo.setup):
require("sqlazo").setup_cmp()
```

### Features

- **Table names**: Suggests all tables in connected database
- **Column names**: Type `tablename.` to get column suggestions
- **Caching**: Schema is cached per connection to avoid repeated queries

### Commands

| Function                                      | Description                      |
| --------------------------------------------- | -------------------------------- |
| `:lua require('sqlazo').get_schema()`         | Fetch schema (returns Lua table) |
| `:lua require('sqlazo').clear_schema_cache()` | Clear cached schemas             |

---

## License

MIT
