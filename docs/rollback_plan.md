# ADF Rollback Plan — Customer 360

> **Per-release runbook for rolling back ADF deployments to Canada Life PROD.**

---

## When to Rollback

Trigger a rollback if any of the following occur after a PROD deployment:
- Pipeline failure rate exceeds 5% within 30 minutes of deployment
- Critical trigger failures (scheduled pipelines not executing)
- Data integrity issues detected in Bronze/Silver/Gold layers
- Integration runtime connectivity failures
- VP Data or Engineering Lead calls for immediate rollback

---

## Rollback Steps

### Step 1 — Identify the Last Known-Good Version

1. Go to the GitHub Actions runs: `https://github.com/Shiva13016/customer-360/actions`
2. Find the last successful `deploy-prod` run
3. Note the **commit SHA** (e.g., `abc1234`)
4. Alternatively, check git tags: `git tag -l 'adf-prod-*' | sort -V | tail -5`

### Step 2 — Trigger Manual Rollback via GitHub Actions

1. Go to **Actions** > **ADF CI/CD - Customer 360** > **Run workflow**
2. Select branch: `main`
3. Enter the target commit SHA in the `target_sha` field
4. Click **Run workflow**
5. The workflow will re-deploy the ARM template from the specified commit

### Step 3 — Verify the Rollback

1. Navigate to Azure Data Factory portal:
   `https://adf.azure.com` → `adf-c360-canadalife`
2. Confirm pipeline count matches the rolled-back version
3. Verify all scheduled triggers are in **Started** state
4. Run a test pipeline execution to confirm connectivity

### Step 4 — Stop/Start Triggers Manually (if needed)

If triggers are in a bad state after rollback:
```bash
# Stop all triggers
bash scripts/validate_arm.sh stop adf-c360-canadalife rg-c360-prod

# Start all triggers
bash scripts/validate_arm.sh start adf-c360-canadalife rg-c360-prod
```

### Step 5 — Notify and Log

1. Notify Engineering Lead and VP Data via Teams/Email
2. Log the incident in **ServiceNow** with:
   - Incident type: ADF Deployment Rollback
   - Deployment run number (GitHub Actions run ID)
   - Rolled-back-to SHA
   - Root cause (if known)
   - Time to detection, time to rollback

---

## Rollback History

| Date | Run # | Rolled Back To SHA | Reason | Resolved By |
|------|-------|-------------------|--------|-------------|
| — | — | — | — | — |

---

## Contacts

| Role | Contact |
|------|---------|
| Engineering Lead | TBD |
| VP Data, Canada Life | TBD |
| ADF Track Engineer | Senior Engineer 1 |
| Azure Subscription Owner | TBD |
