# Lakehouse DAGs

Apache Airflow DAGs for Lakehouse Platform.

## Structure

```
dags/          # Airflow DAG files
requirements.txt  # Python package dependencies
```

## Development

1. Create/Modify DAGs in `dags/` directory
2. Update `requirements.txt` if needed
3. Commit and push to this repository
4. Airflow GitSync will automatically pull the changes

## Airflow Configuration

GitSync settings:
- Repository: https://github.com/SeoHyungjun/lakehouse-dags.git
- Branch: main
- SubPath: dags
- Sync Interval: 60s
