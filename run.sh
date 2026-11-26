#!/usr/bin/env bash
# Dagster builds the tables, Quack on Demand serves them.
#   ./run.sh        build, serve, query
#   ./run.sh stop   stop the recipe's gateway
set -euo pipefail
cd "$(dirname "$0")"

RECIPE_TOOL=dagster
RECIPE_LABEL="Dagster"
SERVE_TARGET=out/warehouse.duckdb

build_pipeline() {
  # Dagster's run storage goes under out/ too, never in your home directory.
  export DAGSTER_HOME="$PWD/out/.dagster"
  mkdir -p "$DAGSTER_HOME"
  uv run --quiet dagster asset materialize --select '*' -f pipeline/definitions.py >out/dagster.log 2>&1 \
    || { tail -n 30 out/dagster.log >&2; die 1 "dagster materialize failed (full log: out/dagster.log)"; }
}

# shellcheck source=lib/recipe.sh
source lib/recipe.sh
recipe_main "$@"
