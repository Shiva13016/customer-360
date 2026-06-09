# Branch Strategy — Customer 360

> Git branching guide for the Customer 360 project at Canada Life.

---

## Branch Overview

| Branch | Purpose | Who Pushes | Protected? |
|--------|---------|-----------|------------|
| `main` | Production-ready code. Source of truth. | Via PR only | Yes — 2 approvals |
| `develop` | Integration branch. All features merge here first. | Via PR only | Yes — 1 approval |
| `feature/adf-*` | ADF Track feature/fix branches | Senior Engineer 1 | No |
| `feature/databricks-*` | Databricks Track branches (future) | Senior Engineer 2 | No |

---

## Flow Diagram

```
main (PROD)
  ^
  |  PR + Dual Approval (Eng Lead + VP Data)
  |
develop (DEV)
  ^
  |  PR + 1 Approval + CI pass
  |
feature/adf-<ticket>-<description>   (ADF Track)
feature/databricks-<ticket>-<desc>   (Databricks Track - future)
```

---

## Naming Convention

| Branch Type | Pattern | Example |
|-------------|---------|---------|
| ADF Feature | `feature/adf-<ticket>-<description>` | `feature/adf-123-add-bronze-pipeline` |
| ADF Hotfix | `hotfix/adf-<ticket>-<description>` | `hotfix/adf-456-fix-trigger-schedule` |
| Databricks Feature | `feature/databricks-<ticket>-<desc>` | `feature/databricks-789-identity-resolution` |

---

## CI/CD Trigger Rules

| Event | Branches | CI Actions |
|-------|---------|------------|
| Push | `feature/adf-*` | Validate ARM, check-vars, quota pre-flight |
| PR to `develop` | From `feature/adf-*` | Full CI + deploy to DEV on merge |
| PR to `main` | From `develop` | Full CI + dual approval gate + deploy to PROD |

---

## Branch Protection Rules

### `main`
- Require 2 approving reviews: Engineering Lead + VP Data (Canada Life)
- Dismiss stale reviews on new commits
- Require status checks to pass: validate-arm, check-vars, quota-preflight
- No direct pushes (even for admins)

### `develop`
- Require 1 approving review
- Require status checks to pass: validate-arm, check-vars
- No direct pushes

---

## Creating a New ADF Feature Branch

```bash
# Always branch from develop
git checkout develop
git pull origin develop

# Create your feature branch
git checkout -b feature/adf-<ticket>-<short-description>

# Work, commit, push
git add .
git commit -m 'feat(adf): your change description'
git push origin feature/adf-<ticket>-<short-description>

# Open PR to develop on GitHub
```

---

## Commit Message Convention

Use conventional commits format:

| Prefix | Use For |
|--------|---------|
| `feat:` | New pipeline, linked service, dataset |
| `fix:` | Bug fix in existing pipeline or trigger |
| `docs:` | Documentation updates |
| `ci:` | Changes to GitHub Actions workflow |
| `chore:` | Maintenance, dependency updates |
