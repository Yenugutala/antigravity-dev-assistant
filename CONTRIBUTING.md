# Contributing Guide — AI Pipeline Accelerator

## Git Workflow

### Branches
- **main** — Protected. Requires PR + architect approval + CI pass.
- **develop** — Daily work. Direct push allowed.
- **feature/*** — Feature branches off develop.

### How to Contribute Code
1. Pull latest from develop: `git pull origin develop`
2. Create a feature branch: `git checkout -b feature/<your-feature>`
3. Write code, tests, commit, push
4. When ready, merge to develop (direct push OK)
5. Periodically, develop is merged to main via PR (architect reviews)

---

## Antigravity Configuration

### What Goes Where

| File | Who Can Edit | Purpose |
|---|---|---|
| `.agents/AGENTS.md` | Architects only (via PR) | Project-wide conventions for all developers |
| `.agents/skills/` | Architects only (via PR) | Shared team skills (build-pipeline, etc.) |

### How to Suggest Changes to AGENTS.md or Shared Skills
1. Make the change on your branch
2. Push to develop (takes effect immediately for all developers)
3. When develop → main PR is created, architects review all `.agents/` changes
4. If the change is rejected, architect corrects it before merging to main

---

## Code Standards

### Python
- snake_case for files and functions
- Type hints on all function signatures
- All `.py` files in `src/` and `notebooks/` start with `# Databricks notebook source`

### SQL
- UPPERCASE keywords (SELECT, FROM, WHERE)
- snake_case for column names

### Testing
- pytest for all tests
- Tests must run locally (no Databricks cluster needed)
- Run before pushing: `pytest tests/ -v`
- Lint before pushing: `ruff check src/ tests/`

---

## Protected Files (CODEOWNERS)

These files require architect approval when merging to main:
- `.github/CODEOWNERS`
- `.agents/AGENTS.md`
- `.agents/skills/`
- `.github/workflows/`

---

## Setting Up Branch Protection on Main (Admin Only)

Go to GitHub → Settings → Branches → Add rule for `main`:
1. Check "Require a pull request before merging"
2. Check "Require approvals" (set to 1)
3. Check "Require review from Code Owners"
4. Check "Require status checks to pass" → select `lint` and `test`
5. Check "Do not allow bypassing the above settings"
