# Contributing to tai-backend-arq

`tai-backend-arq` is an arq **execution backend** for the TAI ecosystem: it
implements `tai_contract.backend.Backend` (the worker runtime, `launch`) and
layers the platform's background-execution features over arq — queued and awaited
tool runs, recurring schedules (interval or crontab) with export/import backup,
and result-chaining callbacks. The hard rule (the plugin rule): **it depends on
`tai-contract` + `tai-kit` only and never imports the skeleton.** Importing
`tai_backend_arq` registers everything through the global `tai_app` handle as a
side-effect (the `ArqBackend`, the `backend_*` tools, and the `sync_task` /
`async_task` / `schedule_task` extensions), and a manifest's `backend_module`
names the package. Fleet propagation of config changes is not this backend's
concern: a backend-runtime process receives fleet ops through the skeleton's own
worker bus, exactly like a serving HTTP worker.

## Ground rules

- **No skeleton import — ever.** The package is contract-facing; the ban is
  enforced by ruff (`flake8-tidy-imports`), so a stray import fails lint:
  ```bash
  grep -rn "tai_skeleton" src/   # must be empty
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

## Dev

```bash
uv sync
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

For local cross-repo work, `make dev` editable-installs the sibling `tai-*`
checkouts this package builds on into the venv. While `[tool.uv.sources]` pins
those siblings to local paths, `uv sync` already installs them editable and
`make dev` changes nothing; once the lock resolves them from the registry,
`uv sync` / `uv run` installs the published builds instead, so re-run
`make dev` afterward to restore the editable links.

Before any commit, run a secret scan over `src/` and `tests/` (e.g.
`detect-secrets scan`).

## License

By contributing you agree your contributions are licensed under Apache-2.0.
