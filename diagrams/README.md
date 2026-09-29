# Architecture Diagrams

Mermaid source for the system design. Each `.md` file is a standalone diagram.

| File | Type | Shows |
|---|---|---|
| `01-high-level-architecture.md` | flowchart | Components, fixed partitions, data stores, notification path |
| `02-processing-pipeline.md` | flowchart | Per-event flow with every decision point (duplicate, backpressure, late event, window re-arm, alert dedupe) |
| `03-runtime-sequence.md` | sequenceDiagram | End-to-end runtime interactions incl. failure paths and rule updates |
| `04-data-model.md` | erDiagram | The three tables, keys, indexes, and their relationship |
| `05-alert-lifecycle.md` | stateDiagram-v2 | Alert status machine and 409 transitions |

## Viewing

- **GitHub/GitLab**: paste the file contents into a Markdown file wrapped in a ` ```mermaid ` fence — it renders natively.
- **VS Code**: install the "Markdown Preview Mermaid Support" extension, or use any Mermaid preview extension on the `.md` file.
- **CLI**: `npx -y @mermaid-js/mermaid-cli -i diagrams/01-high-level-architecture.md -o diagram.svg`
- **Online**: https://mermaid.live — paste the contents of any file.

See `PLAN.md` for the design decisions behind these diagrams and `README.md` for the running text.
