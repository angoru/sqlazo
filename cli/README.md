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

## Configuration

### Connection Priority

sqlazo uses this priority order for database connection settings:
1. File headers (highest priority)
2. Environment variables  
3. `.env` file
4. Default values (lowest priority)

### Environment Variables

```bash
export DB_HOST=localhost
export DB_PORT=3306
export DB_USER=myuser
export DB_PASSWORD=mypassword
export DB_DATABASE=mydatabase
export DB_TYPE=mysql
```

### .env File Support

Create a `.env` file in your working directory:

```
DB_HOST=localhost
DB_PORT=3306
DB_USER=myuser
DB_PASSWORD=mypassword
DB_DATABASE=mydatabase
DB_TYPE=mysql
```

Environment variables will override `.env` file values.

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
