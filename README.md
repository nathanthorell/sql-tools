# sql-tools

A collection of utility tools for working with various dialects of SQL databases.

## Features

- **Object Comparison Tool** (`object_compare`): Compare definitions of stored procedures, views, functions, tables, triggers, sequences, indexes, types, external tables, and foreign keys across different environments (DEV, QA, UAT, PROD)

  - Identify exclusive objects that exist in only one environment
  - Check for definition differences in objects across environments

- **Stored Procedure Tester** (`usp_tester`): Batch test execution of stored procedures with configurable parameters
  - Support for default parameter values
  - Execution time tracking
  - Different logging levels (summary, verbose)

- **View Tester** (`view_tester`): Batch test queries against views
  - Runs a "TOP 1 *" for each view to ensure output is valid
  - Execution time tracking
  - Different logging levels (summary, verbose)

- **Schema Size** (`schema_size`): Analyzes storage across databases by measuring schema sizes.
  - The tool connects to multiple servers, calculates data and index space consumption in megabytes, and generates formatted tabular reports comparing schema sizes.
  - Results are displayed with customizable detail levels based on logging preferences.

- **Data Compare** (`data_compare`): Compare data across different database platforms
  - Support for MSSQL and PostgreSQL databases
  - Compare data using custom SQL queries
  - Option to use query files for complex comparisons
  - Detailed reporting on differences between datasets

## Installation

### Requirements

- Python 3.13+
- Appropriate database drivers:

    - ODBC Driver for SQL Server (for MSSQL databases)
    - More drivers to be added for other database types

### Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/nathanthorell/sql-tools.git
   cd sql-tools
   ```

2. Create and activate a virtual environment, then install the package:

   ```bash
   make install
   ```

   Or manually:

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -e ".[dev]"
   ```

3. Create a `.env` file based on the provided `.env.example`:

   ```bash
   cp .env.example .env
   ```

   Then update the connection strings with your database details.

4. Create a `config.toml` file for each tool you want to use.

## Configuration

### Environment Variables

An `.env.example` file is provided with the repository. Copy this to create your own `.env` file:

```bash
cp .env.example .env
```

Then adjust the connection strings and other settings according to your environment.

The tools will read these environment variables to establish connections to the various SQL instances.

### Tool Configurations

Create a `config.toml` file in the project root directory based on the provided `config-example.toml`:

```bash
cp config-example.toml config.toml
```

- **Object Compare**: Set the schema name to compare across environments
- **USP Tester**: Configure the schema, logging level, and default parameter values for stored procedures
- **View Tester**: Configure the schema and logging level
- **Schema Size**: Configure the server connections, databases to compare, and logging level
- **Data Compare**: Configure named comparison pairs with left/right database connections, database types (MSSQL/PostgreSQL), and queries or query files to compare

## Usage

### Object Comparison Tool

```bash
object_compare
```

This will:

1. Connect to each configured environment using the specified connection strings
1. Compare object definitions across environments for each object type
1. Report differences in object definitions using checksums
1. Highlight objects that exist in one environment but not others

### Stored Procedure Tester

```bash
usp_tester
```

This will:

1. Connect to the configured test database
1. Execute all stored procedures in the specified schema
1. Apply default parameter values
1. Report execution status and timing

### View Tester

```bash
view_tester
```

This will:

1. Connect to the configured test database
1. Execute all views in the specified schema
1. Report execution status and timing

### Schema Size

```bash
schema_size
```

This will:

1. Connect to each server using the specified connection strings
1. Calculate size metrics for each database and schema
1. Generate reports showing data and index sizes per schema
1. Provide comparative summaries across all servers and databases

### Data Compare

```bash
data_compare
```

1. Connect to the configured database sources (supports both MSSQL and PostgreSQL)
1. Execute the defined queries against both data sources
1. Compare the results of both queries
1. Generate a detailed report of matching and non-matching data
1. Display performance comparison between execution time of each data source

## Development

### Linting and Formatting

```bash
make lint    # Run ruff and mypy linters
make format  # Format code with ruff
```

### Clean Up

```bash
make clean   # Remove temporary files and virtual environment
```
