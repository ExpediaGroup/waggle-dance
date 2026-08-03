# Git branch naming

Use this format for new branches:

**`{type}/EGDL-XXXX/<short-name>`**

- `{type}` is the conventional-commit type: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`
- `EGDL-XXXX` is the Jira ticket key. Use `NO-JIRA` for non-ticketed work.
- `<short-name>` is a short, kebab-case description (e.g. `add-search-tool`)

**Examples**:
- `feat/EGDL-7747/add-search-tool`
- `fix/EGDL-8000/trino-connection-timeout`
- `chore/EGDL-7900/bump-deps`
- `chore/NO-JIRA/bump-spring-ai`

When creating or suggesting branch names, always follow this pattern.
