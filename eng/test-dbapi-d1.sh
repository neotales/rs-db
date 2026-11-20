#!/usr/bin/env bash
# Run host codec tests and actual wasm/JS representation tests without a D1 binding.
set -euo pipefail

cd "$(dirname "$0")/.."

host_output=$(cargo test -p neotales-dbapi-d1 --all-targets 2>&1)
printf '%s\n' "$host_output"
grep -Fq 'test codec::tests::json_numbers_outside_javascript_safe_integer_range_are_rejected ... ok' \
  <<<"$host_output"
grep -Fq 'test codec::tests::missing_d1_change_metadata_is_unknown_not_zero ... ok' \
  <<<"$host_output"
grep -Fq 'test codec::tests::d1_batch_result_count_must_match_statement_count ... ok' \
  <<<"$host_output"
grep -Fq 'test codec::tests::d1_cell_conversion_keeps_number_outcomes_explicit ... ok' \
  <<<"$host_output"
grep -Fq 'test codec::tests::decode_raw_requires_one_value_per_column_and_keeps_bytes ... ok' \
  <<<"$host_output"
grep -Fq 'test paging::tests::requery_terminates_after_a_short_final_page_without_looping ... ok' \
  <<<"$host_output"
grep -Fq 'test paging::tests::stopping_on_a_short_page_avoids_the_budget_exhausting_empty_probe ... ok' \
  <<<"$host_output"
grep -Fq 'test paging::tests::an_exact_page_budget_with_a_continuation_fails_closed_before_the_empty_probe ... ok' \
  <<<"$host_output"
grep -Fq 'test paging::tests::an_exact_row_budget_with_a_continuation_fails_closed_before_dispatch ... ok' \
  <<<"$host_output"
grep -Fq 'test paging::tests::a_materializer_error_is_terminal_and_not_retried ... ok' \
  <<<"$host_output"

wasm_output=$(
  CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
    cargo test --target wasm32-unknown-unknown -p neotales-dbapi-d1 2>&1
)
printf '%s\n' "$wasm_output"
grep -Fq 'test codec::tests::actual_js_number_representation_is_checked_before_conversion ... ok' \
  <<<"$wasm_output"
grep -Fq 'test codec::tests::d1_cell_decodes_actual_javascript_values ... ok' \
  <<<"$wasm_output"
if grep -Eq '[1-9][0-9]* failed' <<<"$wasm_output"; then
  echo 'a wasm test failed; refusing to report a pass' >&2
  exit 1
fi
