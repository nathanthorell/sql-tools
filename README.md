# mssql-tools

A collection of utility tools for working with Microsoft SQL Server databases.

## Features

- **Object Comparison Tool** (`obj_compare`): Compare stored procedures and views across different environments (DEV, QA, UAT, PROD)

  - Identify exclusive objects that exist in only one environment
  - Check for definition differences in stored procedures across environments

- **Stored Procedure Tester** (`usp_tester`): Batch test execution of stored procedures with configurable parameters
  - Support for default parameter values
  - Execution time tracking
  - Different logging levels (summary, verbose)

- **View Tester** (`view_tester`): Batch test queries against views
  - Runs a "TOP 1 *" for each view to ensure output is valid
  - Execution time tracking
  - Different logging levels (summary, verbose)

## Installation

### Requirements

- Python 3.13+
- ODBC Driver for SQL Server

### Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/nathanthorell/mssql-tools.git
   cd mssql-tools
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

The tools will read these environment variables to establish connections to your SQL Server instances.

### Tool Configurations

Each tool includes a default `config.toml` file in its respective directory. Adjust the settings according to your specific needs:

- **Object Compare**: Set the schema name to compare across environments
- **USP Tester**: Configure the schema, logging level, and default parameter values for stored procedures
- **View Tester**: Configure the schema and logging level

## Usage

### Object Comparison Tool

```bash
obj_compare
```

This will:

1. Compare stored procedures across all configured environments
2. Find views exclusive to specific environments
3. Compare stored procedure definitions across environments

### Stored Procedure Tester

```bash
usp_tester
```

This will:

1. Connect to the configured test database
2. Execute all stored procedures in the specified schema
3. Apply default parameter values
4. Report execution status and timing

### View Tester

```bash
view_tester
```

This will:

1. Connect to the configured test database
2. Execute all views in the specified schema
3. Report execution status and timing

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
