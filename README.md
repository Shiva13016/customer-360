# Customer 360 — Canada Life

> **Status:** Active Development | ADF Track (Senior Engineer 1) in progress
> > **Project:** Unified Customer 360 Data Platform
> > > **Org:** Canada Life
> > >
> > > ---
> > >
> > > ## Repository Structure
> > >
> > > ```
> > > customer-360/
> > > ├── .github/
> > > │   └── workflows/
> > > │       └── adf_deploy.yml          # GitHub Actions — ADF CI/CD pipeline
> > > ├── adf/
> > > │   ├── arm_templates/              # Exported ADF ARM templates
> > > │   │   └── ARMTemplateForFactory.json
> > > │   ├── linked_services/            # ADF linked service definitions
> > > │   ├── datasets/                   # ADF dataset definitions
> > > │   ├── pipelines/                  # ADF pipeline JSON definitions
> > > │   └── triggers/                   # ADF trigger definitions
> > > ├── scripts/
> > > │   ├── validate_arm.sh             # ARM template pre-flight validation
> > > │   ├── check_vars.py               # Regex check for unresolved ${VAR} patterns
> > > │   └── quota_preflight.sh          # az vm list-skus quota check
> > > ├── docs/
> > > │   ├── rollback_plan.md            # Per-release rollback runbook
> > > │   └── branch_strategy.md          # Git branching guide
> > > └── README.md
> > > ```
> > >
> > > ---
> > >
> > > ## CI/CD — Dual Track Architecture
> > >
> > > | | **ADF Track** | **Databricks Track** |
> > > |---|---|---|
> > > | **Owner** | Senior Engineer 1 | Senior Engineer 2 |
> > > | **Branch Flow** | `feature/adf-*` → `develop` → `main` | `feature/databricks-*` → `develop` → `main` |
> > > | **CI Tool** | GitHub Actions (`adf_deploy.yml`) | Jenkins + pytest (`Jenkinsfile`) |
> > > | **Tests** | ARM validation · `${VAR}` regex · quota pre-flight | 20 tests: 3 unit · 12 integration · 5 DQ regression |
> > > | **Prod Gate** | Dual manual approval: Engineering Lead + VP Data | Dual manual approval: Engineering Lead + VP Data |
> > > | **Status** | ✅ Active | 🔜 Planned |
> > >
> > > > **Why separate tracks?** Different release cadences. An ADF fix at 2am can deploy without waiting for a Databricks notebook review cycle. Decoupling prevents pipeline bottlenecks.
> > > >
> > > > ---
> > > >
> > > > ## Branch Strategy
> > > >
> > > > ```
> > > > main          ← Production-ready, protected. Requires PR + dual approval.
> > > >   └── develop ← Integration branch. All features merge here first.
> > > >         ├── feature/adf-<ticket>-<description>    (ADF Track)
> > > >         └── feature/databricks-<ticket>-<desc>    (Databricks Track — future)
> > > > ```
> > > >
> > > > ### Branch Protection Rules
> > > > - **`main`**: Requires 2 approvals (Engineering Lead + VP Data @ Canada Life). No direct pushes.
> > > > - - **`develop`**: Requires 1 approval. CI must pass before merge.
> > > >   - - **`feature/*`**: Open — developer pushes freely, CI runs on every push.
> > > >    
> > > >     - ---
> > > >
> > > > ## ADF Track — CI/CD Pipeline (`.github/workflows/adf_deploy.yml`)
> > > >
> > > > ### Trigger
> > > > - Push to `feature/adf-*` → runs validation only
> > > > - - PR merged to `develop` → runs validation + deploy to **DEV** environment
> > > >   - - PR merged to `main` → runs validation + deploy to **PROD** (requires dual approval gate)
> > > >    
> > > >     - ### Pipeline Stages
> > > >    
> > > >     - | Stage | Description |
> > > >     - |---|---|
> > > >     - | `validate-arm` | Validates ADF ARM template syntax via `az deployment group validate` |
> > > > | `check-vars` | Regex scans all JSON for unresolved `${VAR}` patterns — fails build if found |
> > > > | `quota-preflight` | Runs `az vm list-skus` to verify compute quota availability before deploy |
> > > > | `deploy-dev` | Deploys ADF ARM template to DEV resource group |
> > > > | `approval-gate` | Manual approval required (Engineering Lead + VP Data) for PROD |
> > > > | `deploy-prod` | Deploys to PROD after approval |
> > > >
> > > > ---
> > > >
> > > > ## Rollback Plan
> > > >
> > > > See [`docs/rollback_plan.md`](docs/rollback_plan.md) for the full per-release runbook.
> > > >
> > > > **Quick Summary:**
> > > > 1. Identify the last known-good ADF ARM template version in Git history
> > > > 2. 2. Trigger `adf_deploy.yml` manually with the target commit SHA as input
> > > >    3. 3. Confirm pipeline count and trigger status in Azure Data Factory portal
> > > >       4. 4. Notify Engineering Lead and log incident in ServiceNow
> > > >         
> > > >          5. ---
> > > >         
> > > >          6. ## Getting Started
> > > >         
> > > >          7. ```bash
> > > > # Clone the repo
> > > > git clone https://github.com/Shiva13016/customer-360.git
> > > > cd customer-360
> > > >
> > > > # Create your ADF feature branch
> > > > git checkout develop
> > > > git pull origin develop
> > > > git checkout -b feature/adf-<ticket>-<short-description>

# After changes, push and open PR to develop
git push origin feature/adf-<ticket>-<short-description>
```

---

## Contacts

| Role | Responsibility |
|---|---|
| Senior Engineer 1 | ADF Track — pipelines, ARM templates, GitHub Actions |
| Senior Engineer 2 | Databricks Track — notebooks, pytest, Jenkins (future) |
| Engineering Lead | PR approver, Production gate |
| VP Data, Canada Life | Production gate (dual approval) |
