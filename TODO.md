# TODO — remaining work (verified against code, 2026-09-11)

Derived from `ACTION_LIST.md`. Items already landed on `feat/structural-design-changes`
are omitted (1–6, 12, 13, 16–18, 20, 21, 26). Order is execution order.

Legend: **[S]** hours · **[M]** 1–2 days · **[L]** week+

---

## Now — small fixes, no design decisions needed

- [x] **24. CI** [S] — `.github/workflows/` is empty. Add `ci.yml` running
      `go build ./... && go test ./...` in `orchestrator/` and
      `python -m unittest discover -s executor/tests -p "test_*.py"`.
      Lands first so nothing below regresses.
- [x] **14. Error-path context** [S] — `executor/worker/server.py:75-82`: 400/503
      responses send `{status, error}` only. Add `run_id` and `step_key` when
      parseable (they are on the 200 path via `StepResult` already).
- [x] **22. SQL guard hardening** [S] — `executor/connectors/postgres.py:79`
      `_reject_non_select` is a prefix check; `WITH ... INSERT` CTE bypasses it.
      Reject data-modifying keywords inside CTEs, or parse with `sqlglot`/`pglast`.
      Optionally validate referenced tables/columns against `catalog.Context` schema.
- [x] **20b. Fail-closed auth** [S] — `_authorized()` returns `True` when
      `WORKER_TOKEN` is empty; allowlists likewise disabled by default. Add a
      `WORKER_INSECURE_DEV=1` escape hatch and refuse to start otherwise.

## Next — Phase C, the actual feature

- [ ] **8. Model + prompt config rows** [S] — migration 5: `interpreter_configs`
      `{id, provider, model, sys_prompt, temperature, max_tokens, version}`.
      Record `config_id` on each interpret result / degradation event.
- [ ] **7. NLP service `:8091`** [S] — one synchronous Python process. Returns
      `{pipeline, confidence, warnings}` matching `internal/interpreter/client.go`.
      `task_type` handler map inside the service, not a routing tier.
- [ ] **9. Prompt grounding** [S] — build prompt from the `catalog.Context` the
      orchestrator already sends (connectors, destinations, transform ops, schema).
      Add 3–5 worked query → pipeline JSON examples. Use schema-constrained /
      tool-mode output, not "reply in JSON". Mechanical rules over targets.
- [ ] **11. Eval set** [M] — 50–100 `(query, pipeline JSON)` pairs under `tests/eval/`.
      Cover single-source, multi-step, ambiguous/unanswerable. Score structural
      match and config correctness separately.
- [ ] **10. Confidence calibration experiment** [M–L] — compare self-reported,
      log-probs, self-consistency (5×), validation-derived. Report gate
      precision/recall against the eval set. This is the differentiator.
- [ ] **15. Interpret → create loop** [S] — `/interpret?persist=true` (or body
      flag) creates the pipeline when validation passes; still lands as
      `requires_approval` if it has a `load` step (rule 3.1).

## Then — polish

- [ ] **25. Docker + compose** [S] — orchestrator, worker, Postgres, NLP service.
      Needed to run 27 with `EXEC_MODE=worker`.
- [ ] **27. Integration test** [M] — one real path: interpret → create → approve →
      run → assert rows landed. `tests/` is currently empty;
      `examples/postgres_e2e.py` drives the runner directly and is not a test.
- [ ] **28. UI** [L] — `ui/` empty. Last; depends on everything above.

## Skip / defer (decided)

- **19. Celery Beat** — optional; zombie sweeper already covered by lease expiry.
- **23. Real auth + tenancy** — out of scope; document as future work.
