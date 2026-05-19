# taxi_rides_ny dbt Core setup

This project now runs with dbt Core + the BigQuery adapter instead of dbt Cloud CLI.

## What changed

- `dbt_project.yml` now points at the local `taxi_rides_ny` profile.
- `profiles.yml` is a dbt Core profile that uses the BigQuery service-account JSON keyfile flow.
- `.vscode/settings.json` tells the dbt Power User extension to use Core mode and the project Python interpreter.

## One-time setup

1. Make sure your virtual environment has dbt installed, for example `dbt-bigquery`.
2. Keep the BigQuery service-account key at `~/.dbt/bigquery-creds.json` or replace that path in `profiles.yml`.
3. Open this workspace in VS Code so the folder settings are picked up.

## How to run dbt Core

From the project directory:

```bash
export DBT_PROFILES_DIR=/home/penny_dev/projects/de-zoomcamp/04-analytics-engineering/taxi_rides_ny
dbt debug
dbt deps
dbt build
```

If you use a different key location, update `profiles.yml` accordingly or point `GOOGLE_APPLICATION_CREDENTIALS` at the JSON file and switch the profile to read from that path.

## VS Code / dbt Power User

- Keep `dbt.dbtIntegration` set to `core`.
- Keep `dbt.dbtPythonPathOverride` pointed at the project venv Python executable.
- If the extension cannot find your profile, check that `DBT_PROFILES_DIR` matches the folder containing `profiles.yml`.
