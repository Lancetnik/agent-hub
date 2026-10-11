# Domain docs

This project uses a **single-context** layout: one root `GLOSSARY.md` and `docs/adr/` for architectural decisions.

## Before exploring the codebase

- Read `GLOSSARY.md` at the project root.
- Read ADRs in `docs/adr/` that concern the area you are about to work in.

If these files are absent, proceed silently. The `/domain-modeling` skill, reached via `/grill-with-docs` and `/improve-codebase-architecture`, creates them lazily when terms or decisions are resolved.

## File structure

```text
/
├── GLOSSARY.md
├── docs/
│   └── adr/
│       └── NNNN-<decision-slug>.md
└── src/
```

## Use the glossary's vocabulary

Use terms as defined in `GLOSSARY.md` when naming domain concepts in issue titles, refactor proposals, hypotheses, or tests.

If a needed concept is absent, reconsider whether the project uses that concept or note the gap for `/domain-modeling`.

## Flag ADR conflicts

If a proposal contradicts an existing ADR, identify the ADR and explain why the decision merits reopening.
