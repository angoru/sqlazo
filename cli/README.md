# sqlazo CLI

Execute SQL queries from files with connection headers.

## Installation

```bash
cd cli
pip install -e .
```

## Quick Start

```sql
-- dbtype: mysql
-- host: db.example.local
-- port: 3306
-- user: app_user
-- database: app_db

SELECT * FROM users LIMIT 10;
```

```bash
sqlazo query.sql
```

## Supported Databases

- MySQL
- MariaDB
- PostgreSQL
- SQLite

## Configuration

### Connection Priority

sqlazo uses this priority order for database connection settings:
1. File headers (highest priority)
2. Environment variables
3. `.env` file
4. Default values (lowest priority)

Database type must be specified via URL, `DB_TYPE`, or a header key like `db_type`.

### Environment Variables

```bash
export DB_HOST=localhost
export DB_PORT=3306
export DB_USER=app_user
export DB_PASSWORD='<set outside committed files>'
export DB_DATABASE=app_db
export DB_TYPE=mysql
```

### File Header Example

```sql
-- db_type: postgresql
-- host: localhost
-- db: mydb

SELECT 1;
```

### .env File Support

Create a `.env` file in your working directory:

```
DB_HOST=localhost
DB_PORT=3306
DB_USER=app_user
DB_PASSWORD=<set outside committed files>
DB_DATABASE=app_db
DB_TYPE=mysql
```

Environment variables will override `.env` file values.

## CLI Options

```bash
sqlazo file.sql              # Table format (default)
sqlazo file.sql -f record    # One field per line
sqlazo file.sql -f json      # JSON output
sqlazo file.sql -f csv       # CSV output
sqlazo file.sql -f json-meta # JSON with metadata for editor integrations
sqlazo file.sql --schema     # JSON schema for autocomplete
sqlazo file.sql -v           # Verbose
```

## License

MIT
