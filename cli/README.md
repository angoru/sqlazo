# sqlazo CLI

Execute SQL queries from files with connection headers.

## Installation

```bash
cd cli
pip install -e .
```

## Quick Start

```sql
-- url: mysql://user:pass@localhost:3306/mydb

SELECT * FROM users LIMIT 10;
```

```bash
sqlazo query.sql
```

## Supported Databases

- MySQL
- PostgreSQL  
- SQLite
- MongoDB
- Redis

## CLI Options

```bash
sqlazo file.sql              # Table format (default)
sqlazo file.sql -f record    # One field per line
sqlazo file.sql -f json      # JSON output
sqlazo file.sql -f csv       # CSV output
sqlazo file.sql -v           # Verbose
```

## License

MIT
