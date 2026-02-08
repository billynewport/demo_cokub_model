---
name: Enable SQL Server CDC
description: Enable Change Data Capture on a SQL Server database and its tables. Covers Agent setup, database/table enablement, and verification.
---

# Enable SQL Server Change Data Capture (CDC)

This skill enables CDC on a SQL Server instance so that INSERT, UPDATE, and DELETE operations are captured for downstream ingestion by DataSurface.

## IMPORTANT: Execution Rules

1. **Execute steps sequentially** - Do not skip ahead or combine steps
2. **Verify each step** - Confirm success before proceeding
3. **Ask for missing information** - If connection details or table names are not provided, ask the user
4. **Report failures immediately** - If any step fails, stop and troubleshoot

## Prerequisites

- `sqlcmd` installed locally
- Network access to the SQL Server instance
- `sa` or sysadmin-level credentials
- SSH access to the SQL Server host (if Agent needs to be enabled)

Ask the user for these values if not already provided:

```bash
SQL_HOST              # SQL Server hostname (e.g., sqlserver-co)
SQL_USER              # SQL Server login (default: sa)
SQL_PASSWORD          # SQL Server password
SQL_DATABASE          # Database to enable CDC on (e.g., customer_db)
SSH_USER              # SSH username for the SQL Server host (for Agent setup)
```

## Step 1: Verify connectivity

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -Q "SELECT @@VERSION"
```

## Step 2: Check SQL Server Agent status

CDC requires SQL Server Agent to be running. The Agent runs the capture job that polls the transaction log every 5 seconds.

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d master -Q \
  "SELECT servicename, status_desc, startup_type_desc FROM sys.dm_server_services"
```

**Expected:** `SQL Server Agent (MSSQLSERVER)` should show `Running`.

If the Agent is **Stopped**, enable it on the host:

### Linux SQL Server (systemd)

```bash
ssh $SSH_USER@$SQL_HOST 'sudo /opt/mssql/bin/mssql-conf set sqlagent.enabled true && sudo systemctl restart mssql-server'
```

**WARNING:** This restarts SQL Server and briefly interrupts all connections. Confirm with the user before proceeding.

After restart, verify the Agent is running:

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d master -Q \
  "SELECT servicename, status_desc FROM sys.dm_server_services"
```

### Docker SQL Server

```bash
docker exec <container> /opt/mssql/bin/mssql-conf set sqlagent.enabled true
docker restart <container>
```

### Windows SQL Server

```powershell
# Start Agent via SQL Server Configuration Manager or:
net start SQLSERVERAGENT
```

**Checkpoint:** SQL Server Agent shows `Running`.

## Step 3: Check current CDC status

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "SELECT name, is_cdc_enabled FROM sys.databases WHERE name = '$SQL_DATABASE'"
```

If `is_cdc_enabled = 1`, skip to Step 5 to check table-level CDC.

## Step 4: Enable CDC on the database

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "EXEC sys.sp_cdc_enable_db;"
```

Verify:

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "SELECT name, is_cdc_enabled FROM sys.databases WHERE name = '$SQL_DATABASE'"
```

**Checkpoint:** `is_cdc_enabled = 1`.

## Step 5: Check table-level CDC status

List all user tables and their CDC tracking status:

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "SELECT name, is_tracked_by_cdc FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND type = 'U'"
```

Ask the user which tables to enable CDC on if not already specified.

## Step 6: Enable CDC on tables

For each table that needs CDC:

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q "
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = '<TABLE_NAME>',
    @role_name = NULL;
"
```

**Expected output:**
```
Job 'cdc.<database>_capture' started successfully.
Job 'cdc.<database>_cleanup' started successfully.
```

The capture/cleanup job messages only appear for the first table enabled. Subsequent tables share the same jobs.

**Note:** The `Update mask evaluation will be disabled` warning is harmless and can be ignored (CLR not needed for CDC).

## Step 7: Verify CDC is fully configured

### Database level

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "SELECT name, is_cdc_enabled FROM sys.databases WHERE name = '$SQL_DATABASE'"
```

### Table level

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "SELECT name, is_tracked_by_cdc FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo') AND type = 'U'"
```

### Capture instances

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "SELECT capture_instance, object_id FROM cdc.change_tables"
```

Capture instances follow the naming convention `<schema>_<table>` (e.g., `dbo_customers`).

### CDC jobs

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d msdb -Q \
  "SELECT name, enabled, description FROM msdb.dbo.sysjobs WHERE name LIKE 'cdc.%'"
```

**Checkpoint:** All of the following should be true:
- Database `is_cdc_enabled = 1`
- All target tables show `is_tracked_by_cdc = 1`
- Capture instances exist in `cdc.change_tables`
- CDC capture and cleanup jobs are present and enabled

## Step 8: Test that changes are being captured

Make a test change and verify it appears in the CDC table:

```bash
# Insert a test row (adjust for your table schema)
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "INSERT INTO dbo.<TABLE_NAME> (<columns>) VALUES (<values>)"

# Wait for Agent to capture (polls every 5 seconds)
sleep 10

# Check CDC capture table for the change
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "DECLARE @from_lsn binary(10) = sys.fn_cdc_get_min_lsn('dbo_<TABLE_NAME>');
   DECLARE @to_lsn binary(10) = sys.fn_cdc_get_max_lsn();
   SELECT TOP 5 * FROM cdc.fn_cdc_get_all_changes_dbo_<TABLE_NAME>(@from_lsn, @to_lsn, 'all update old');"
```

**Operation codes in `__$operation`:**
- 1 = DELETE
- 2 = INSERT
- 3 = UPDATE (before image)
- 4 = UPDATE (after image)

## Troubleshooting

### CDC enable fails with "SQLServerAgent is not currently running"

SQL Server Agent must be running before enabling CDC. Go back to Step 2.

### Agent won't start on Linux

Check the mssql-conf setting:

```bash
ssh $SSH_USER@$SQL_HOST 'sudo /opt/mssql/bin/mssql-conf get sqlagent.enabled'
```

If it shows no setting or `false`, set it to `true` and restart:

```bash
ssh $SSH_USER@$SQL_HOST 'sudo /opt/mssql/bin/mssql-conf set sqlagent.enabled true && sudo systemctl restart mssql-server'
```

### CDC changes not appearing

1. **Check Agent is running:**

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d master -Q \
  "SELECT servicename, status_desc FROM sys.dm_server_services WHERE servicename LIKE '%Agent%'"
```

2. **Check CDC jobs are enabled:**

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d msdb -Q \
  "SELECT name, enabled FROM msdb.dbo.sysjobs WHERE name LIKE 'cdc.%'"
```

3. **Check for CDC errors in job history:**

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d msdb -Q \
  "SELECT TOP 10 j.name, h.step_name, h.message, h.run_date, h.run_time
   FROM msdb.dbo.sysjobhistory h
   JOIN msdb.dbo.sysjobs j ON h.job_id = j.job_id
   WHERE j.name LIKE 'cdc.%' AND h.run_status != 1
   ORDER BY h.run_date DESC, h.run_time DESC"
```

### LSN gap / retention expired

CDC has a default retention of 3 days. If ingestion falls behind, old changes are purged:

```bash
# Check current retention (minutes)
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "EXEC sys.sp_cdc_help_jobs"
```

To increase retention (e.g., to 7 days = 10080 minutes):

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "EXEC sys.sp_cdc_change_job @job_type = 'cleanup', @retention = 10080"
```

### Disabling CDC

To disable CDC on a specific table:

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q "
EXEC sys.sp_cdc_disable_table
    @source_schema = 'dbo',
    @source_name = '<TABLE_NAME>',
    @capture_instance = 'dbo_<TABLE_NAME>';
"
```

To disable CDC on the entire database (disables all tables):

```bash
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q \
  "EXEC sys.sp_cdc_disable_db;"
```

### Re-enabling CDC after schema changes

If the source table schema changes (columns added/removed), CDC must be refreshed:

```bash
# Disable CDC on the table
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q "
EXEC sys.sp_cdc_disable_table
    @source_schema = 'dbo',
    @source_name = '<TABLE_NAME>',
    @capture_instance = 'dbo_<TABLE_NAME>';
"

# Re-enable CDC on the table
sqlcmd -S $SQL_HOST -U $SQL_USER -P $SQL_PASSWORD -C -d $SQL_DATABASE -Q "
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = '<TABLE_NAME>',
    @role_name = NULL;
"
```

**Important:** This resets the capture instance. Any unprocessed changes from before the reset will be lost. Coordinate with DataSurface ingestion to ensure all changes are consumed before re-enabling.

## Quick Reference

| Component | Check Command |
|-----------|--------------|
| Agent status | `SELECT servicename, status_desc FROM sys.dm_server_services` |
| Database CDC | `SELECT is_cdc_enabled FROM sys.databases WHERE name = '<db>'` |
| Table CDC | `SELECT is_tracked_by_cdc FROM sys.tables WHERE name = '<table>'` |
| Capture instances | `SELECT capture_instance FROM cdc.change_tables` |
| Min LSN | `SELECT sys.fn_cdc_get_min_lsn('dbo_<table>')` |
| Max LSN | `SELECT sys.fn_cdc_get_max_lsn()` |
| CDC jobs | `SELECT name, enabled FROM msdb.dbo.sysjobs WHERE name LIKE 'cdc.%'` |
| Retention | `EXEC sys.sp_cdc_help_jobs` |
