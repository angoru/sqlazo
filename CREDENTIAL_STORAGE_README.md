# sqlazo - Encrypted Credential Storage Guide

This guide explains how to securely store and use database credentials with sqlazo's encrypted credential storage.

## Overview

sqlazo provides encrypted credential storage to protect sensitive database connection information. Credentials are encrypted using a user-defined password and stored securely on your system.

## Prerequisites

- sqlazo CLI installed (version 0.1.4+)
- Python 3.11+
- cryptography library (automatically installed with sqlazo)

## Storing Credentials

To store credentials in an encrypted profile:

```bash
sqlazo cred store <profile_name> [options]
```

### Options:
- `--host HOST` - Database host
- `--port PORT` - Database port
- `--user USER` - Database username
- `--password PASSWORD` - Database password
- `--database DATABASE` - Database name
- `--db-type DB_TYPE` - Database type (mysql, postgresql, sqlite, mongodb, redis)

### Example:
```bash
sqlazo cred store my_postgres_db \
  --host localhost \
  --port 5432 \
  --user myuser \
  --password mypass \
  --database mydatabase \
  --db-type postgresql
```

You will be prompted to enter a master password for encryption. Remember this password as you'll need it to retrieve the credentials later.

## Listing Stored Profiles

To see all stored credential profiles:

```bash
sqlazo cred list
```

## Using Stored Credentials

To execute a query using stored credentials:

```bash
sqlazo query --profile <profile_name> <sql_file>
```

### Example:
```bash
sqlazo query --profile my_postgres_db my_query.sql
```

You will be prompted to enter the master password to decrypt the credentials.

## Retrieving Stored Credentials

To view stored credentials (without using them):

```bash
sqlazo cred retrieve <profile_name>
```

You will be prompted to enter the master password.

## Deleting Stored Credentials

To remove a stored profile:

```bash
sqlazo cred delete <profile_name>
```

## Using with Neovim Plugin

The Neovim plugin also supports using stored credentials:

### Commands:
- `:SqlazoRun <profile>` - Execute current query with specified profile
- `:SqlazoRunVertical <profile>` - Execute in vertical split with profile
- `:SqlazoRunHorizontal <profile>` - Execute in horizontal split with profile
- `:SqlazoRunRecord <profile>` - Execute with record format and profile
- `:SqlazoStoreCreds <profile> <host> [port] [user] [password] [database] [db_type]` - Store credentials
- `:SqlazoListCreds` - List stored profiles
- `:SqlazoRetrieveCreds <profile>` - Retrieve stored credentials

### Example:
```vim
:SqlazoRun my_postgres_db
```

## Configuration

You can set a default profile in your Neovim configuration:

```lua
require("sqlazo").setup({
  format = "table",
  split = "float",
  profile = "my_default_profile",  -- Set default profile
  safe_mode = true,
})
```

With a default profile set, commands like `:SqlazoRun` will automatically use the specified profile unless overridden.

## Security Notes

- Choose a strong master password for encrypting credentials
- Store the master password securely and separately from your system
- The encrypted credentials are stored in `~/.config/sqlazo/credentials/`
- Each installation generates a unique salt for added security
- If you forget your master password, you will not be able to retrieve stored credentials

## Troubleshooting

### "Wrong password" error
If you receive a "wrong password" error when trying to retrieve credentials, ensure you're entering the exact master password used when storing the credentials.

### "Profile not found" error
Make sure the profile name is spelled correctly and exists in the stored credentials list.

### Permission errors
Ensure you have read/write permissions to the `~/.config/sqlazo/credentials/` directory.

## Best Practices

1. Use different master passwords for different environments (development, staging, production)
2. Regularly rotate stored credentials
3. Limit access to systems where encrypted credentials are stored
4. Use environment-specific profiles to avoid accidentally connecting to the wrong database