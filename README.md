# twicerun

Run a batch pipeline several times on the same input and report, per step, how often it failed to give the same answer.

The premise is one measurement. `SELECT g, sum(v) FROM t GROUP BY g` over 2,000,000 rows in 1,000 groups, run twice in the same DuckDB session at `threads=8`, disagrees on several hundred of the 1,000 groups. Nothing is wrong with the query. Parallel float reduction associates in whatever order the threads finish in, and float addition is not associative. So a rerun checker built on exact equality reports hundreds of findings on correct code, which is why the comparison, not the runner, is the project.

Status: in progress. Slice 1 of 7.

## Running it

```bash
git clone https://github.com/DataScienceVishal/twicerun.git
cd twicerun
uv sync --all-extras
./scripts/install-hooks.sh
```

The test suite runs with no credentials and no network:

```bash
uv run pytest
```

This project makes no model calls. There is no LLM adapter, no `openai` dependency and no `AZURE_OPENAI_*` configuration, because a non-deterministic output layer on a tool about determinism would be a contradiction.
