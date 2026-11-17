# Pehle yeh steps karo

This is a new project. Extract it separately from your loan-portfolio project.

1. Open pgAdmin using your working administrator connection. Open Query Tool on `postgres` and run:

```sql
CREATE DATABASE payments_recon_db OWNER loan_risk_app;
```

2. Open PowerShell inside the extracted `digital-payments-reconciliation` folder:

```powershell
py -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

3. Edit `.env`. Put in the password that already works for `loan_risk_app`. Keep the new database name `payments_recon_db`. Do not copy the old loan database name.

4. Run:

```powershell
.\.venv\Scripts\python.exe scripts\run.py init
.\.venv\Scripts\python.exe scripts\run.py generate
```

You should see a schema-created message and a generated-input summary. Then follow README steps 4 and 5 for the initial reconciliation and the late-settlement resolution.

If database authentication fails, compare the role/password/port with your working pgAdmin connection. Passwords do not need to be changed to create a new database for the same role.
