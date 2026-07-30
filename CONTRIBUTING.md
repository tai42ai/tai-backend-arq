# Contributing to tai42-backend-arq

`tai42-backend-arq` is an arq **execution backend** for the TAI ecosystem: it
implements `tai42_contract.backend.Backend` (the worker runtime, `launch`) and
layers the platform's background-execution features over arq — queued and awaited
tool runs, recurring schedules (interval or crontab) with export/import backup,
and result-chaining callbacks. The hard rule (the plugin rule): **it depends on
`tai42-contract` + `tai42-kit` only and never imports the skeleton.** Importing
`tai42_backend_arq` registers everything through the global `tai42_app` handle as a
side-effect (the `ArqBackend`, the `backend_*` tools, and the `sync_task` /
`async_task` / `schedule_task` extensions), and a manifest's `backend_module`
names the package. Fleet propagation of config changes is not this backend's
concern: a backend-runtime process receives fleet ops through the skeleton's own
worker bus, exactly like a serving HTTP worker.

## Ground rules

- **No skeleton import — ever.** The package is contract-facing; the ban is
  enforced by ruff (`flake8-tidy-imports`), so a stray import fails lint:
  ```bash
  grep -rn "tai42_skeleton" src/   # must be empty
  ```
- **No control plane in the backend.** Fleet ops arrive over the app's worker
  bus; this backend never fans control operations out itself.
- **Loud errors.** No swallowed exceptions, silent fallbacks, or silent
  truncation. A failed job re-raises as `TaskFailedError`; per-row schedule
  import errors are surfaced as `{"index", "name", "error"}`, never swallowed;
  capabilities arq has no reliable data model for raise `NotImplementedError`.
- **Typed package** (`py.typed`). Pyright runs clean.

## Layout

- `backend.py` — `ArqBackend` (the `Backend` impl) and its registration.
- `worker.py`, `pool.py`, `lifecycle.py` — the worker CLI/runtime, the shared
  ArqRedis pool, and the shutdown hook.
- `tasks.py`, `extensions.py`, `callback.py`, `signatures.py` — queued dispatch,
  the `sync_task` / `async_task` / `schedule_task` extensions, callback chaining,
  and dispatch signatures.
- `tools.py` — the `backend_*` tool surface.
- `scheduler.py`, `records.py` — recurring schedules and portable schedule
  records for backup.
- `settings.py` — the `ARQ_` settings.

## Naming

PyPI is a flat namespace with no owner in the path, so distributions carry the
`tai42-` prefix. GitHub repositories keep their `tai-` names, because the
`tai42ai` organisation already namespaces them. Import packages follow the
distribution.

| Surface | Form |
| --- | --- |
| Distribution — PyPI, `pip install`, dependency pins | `tai42-<name>` |
| Import package | `tai42_<name>` |
| GitHub repository | `tai-<name>` |

So a dependency is declared as `tai42-<name>` while its repository is named
`tai-<name>`, and both spellings are correct in their own context.

Some surfaces are deliberately neither, and must not be renamed: the `tai` CLI
command (`tai42` is an alias), the Prometheus metric namespace (`tai_tool_*`),
`TAI_*` environment variables, and the `tai-plugin.yml` descriptor filename.

## Dev

```bash
uv venv --python 3.13
uv pip install --no-sources --group dev --editable .
uv run --no-sync pytest --cov --cov-report=term-missing
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync pyright
```

`make dev` installs the sibling `tai-contract` and `tai-kit` repos as editable installs for local cross-repo development.

Before any commit, run a secret scan over `src/` and `tests/` (e.g.
`detect-secrets scan`).

## Dependency resolution

`uv.lock` pins the `tai42-*` siblings to their released index versions while `[tool.uv.sources]` points them at local `../tai-*` checkouts. The two disagree deliberately: CI sets `UV_NO_SOURCES=1` and asserts the lock with `uv sync --locked`, so it resolves the artifacts a user installs. A bare `uv lock` beside sibling checkouts re-couples the lock to editable path entries, which then fails that `--locked` check — run `uv lock --no-sources` instead. See [How dependencies resolve](https://tai42.ai/contributing#how-dependencies-resolve).

## License

By contributing you agree your contributions are licensed under Apache-2.0.
