# Payments DAG in the existing loan-risk Airflow stack

This update reuses the running `loan-risk-platform` Airflow services and UI at http://localhost:8081. It adds a DAG and file mounts; it does not create another Airflow stack or database.

## Install on Windows

1. Extract `payments-airflow-update.zip` into your existing `digital-payments-reconciliation` project root. Merge folders if prompted. Keep your existing root `.env`, source files, settlements, and database.
2. Keep Docker Desktop and the loan-risk Airflow services running. In PowerShell, from the payments project root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install_payments_airflow.ps1
```

The installer discovers the loan Airflow folder from the running worker container's Compose label. It writes an additive `docker-compose.override.yaml` there and validates Compose configuration. It preserves unrelated existing override files and stops rather than overwriting them.

If folder discovery fails, specify the actual loan project's Airflow directory:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install_payments_airflow.ps1 -LoanAirflowDir "C:\Projects\loan-portfolio-early-warning\airflow"
```

The override contains absolute paths to the existing payments project, not passwords. Those absolute paths are local deployment configuration and need regenerating if the payments folder is moved.

3. The installer prints the correct `cd` command for your loan Airflow folder. Before the next command, wait until any existing DAG tasks are idle; applying changed mounts can recreate worker/API/scheduler containers.

```powershell
cd "C:\Projects\loan-portfolio-early-warning\airflow"
docker compose up -d
```

Use the actual path printed by the installer if different. Do not execute that Compose command from the payments `kafka` folder.

4. Open http://localhost:8081, sign in, and find `payments_settlement_reconciliation`. Unpause and trigger it. The existing `loan_portfolio_early_warning` remains available on the same UI.

## Workflow

`check_inputs` → `load_settlements` → `reconcile_now`

- `check_inputs` confirms mounted files, settlement CSV availability, and a payments-database connection with at least one ingested payment event.
- `load_settlements` loads all CSV files currently in `data/settlements` through the existing CLI. Identical successful content is skipped by the ingestion ledger. It does not ingest payment-event files.
- `reconcile_now` runs the existing rules at current UTC processing time. The result is saved as a new reconciliation-run record.

Schedule is every 30 minutes, with 2 retries per task, 5 minutes apart, and at most one active DAG run. Existing source files are sufficient for a manual validation run. Add a new settlement CSV under the local payments folder for a future scheduled ingestion.

## Database isolation

The loan Airflow environment contains loan database settings. Task helpers deliberately discard inherited `DB_*` settings for the subprocess and instead read the payments project's `.env` with interpolation disabled. They require `DB_NAME=payments_recon_db` and supply `DB_HOST=host.docker.internal` inside Docker.

Local Python and Power BI can retain `DB_HOST=127.0.0.1` in the payments `.env`. The local `.env` is read-only in the container. No password is included in the generated override or CLI arguments. The root project is mounted read-only, with only its data directory mounted writable for validation reports.

The helper reuses `python-dotenv` and `psycopg2` already installed in the loan-risk image. It does not need kafka-python because Kafka ingestion remains a separate Windows process.

## Kafka and Power BI behavior

The DAG does not keep the Kafka consumer running. Run it independently whenever you publish new payment events. The previous Kafka consumer's default 15-second idle exit still applies. The DAG only reconciles events already stored in PostgreSQL.

Power BI Desktop Import mode still requires Refresh. This update does not automate an open PBIX file.

## First-run expectations

With the completed demo data, all 3 tasks should turn green. Settlement batches already loaded should log SKIPPED. Reconciliation should retain 7 open exceptions and 91 matched payments if no new records were added.

The as-of timestamp now advances from the fixed January demo date to current UTC. After this, the CLI intentionally rejects older demo timestamps that would move current exception state backward. Use a current/later timestamp for further manual reconciliation. Do not reset your database to solve that safeguard.

Verify in pgAdmin, connected to payments_recon_db:

```sql
SELECT * FROM recon.vw_run_summary
ORDER BY completed_at DESC
LIMIT 5;

SELECT status, COUNT(*)
FROM recon.exception
GROUP BY status;
```

## Troubleshooting

From the loan project's `airflow` folder:

```powershell
docker compose logs --tail 80 airflow-dag-processor airflow-worker
```

If the DAG appears but a task fails, use that task's Airflow log for the exact error. Windows-to-PostgreSQL connectivity and Docker-to-Windows connectivity are different paths; the host database must also accept the Docker connection.

If an unrelated custom Compose override already exists, the installer leaves it unchanged. Review it before merging the three payments mounts into the affected services. Do not delete an existing override without checking its purpose.

## Verification limits

Twenty pure-Python tests pass, including four new checks for payments task environment isolation and invalid credentials/configuration. DAG syntax and generated override structure were checked. Docker and PowerShell are unavailable in the build environment, so live installation, Docker Compose merging, DAG import, and database execution must be verified on your Windows PC.
