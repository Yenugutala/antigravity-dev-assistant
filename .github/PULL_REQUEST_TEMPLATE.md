## Summary
<!-- 1-3 bullet points describing what this PR does -->

-

## Type of Change
<!-- Check all that apply -->

- [ ] Feature (new functionality)
- [ ] Bug fix
- [ ] Pipeline code (Bronze/Silver/Gold)
- [ ] Test updates
- [ ] Documentation
- [ ] Claude config change (.claude/ files)

## Claude Config Changes
<!-- If you modified any .claude/ files, fill this section. Otherwise delete it. -->

**Files changed:**
-

**Why:**
<!-- Explain what was wrong or what improvement this makes -->

**Impact:**
<!-- Who does this affect? All developers? Only one layer? -->

## Testing
<!-- How did you verify this works? -->

- [ ] `pytest tests/ -v` passes
- [ ] `ruff check src/ tests/` passes
- [ ] Tested on Databricks (if pipeline code)

## Checklist
- [ ] No hardcoded secrets or credentials
- [ ] No direct changes to main/master branch
- [ ] All .py files start with `# Databricks notebook source` (if in src/ or notebooks/)
- [ ] Commit messages are clear and descriptive
