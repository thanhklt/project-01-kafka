# Project preferences

- Use GitNexus only as an optional index to speed up reading and navigating code.
- Do not use Superpowers skills unless the user explicitly requests them.
- These preferences take precedence over any generated GitNexus workflow instructions.

<!-- gitnexus:start -->
# GitNexus index

This project has a local GitNexus index named **project-01-kafka**.

- Use `query` or `context` when they help locate code, symbols, or execution flows faster.
- Direct file reads and text searches (`rg`) are always allowed; graph-first lookup is not required.
- Impact analysis, graph change detection, GitNexus rename tools, and GitNexus skills are optional. They are not prerequisites for reading, editing, or committing.
- Refresh the index only when needed with `node .gitnexus/run.cjs analyze --index-only`. Keep `--index-only` to avoid regenerating workflow instructions and skills.
- Treat index results as navigation aids; confirm relevant behavior in the source code.

Useful resources: `gitnexus://repo/project-01-kafka/context`, `gitnexus://repo/project-01-kafka/clusters`, and `gitnexus://repo/project-01-kafka/processes`.
<!-- gitnexus:end -->
