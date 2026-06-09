# Manual Real Provider Smoke Runbook

## Purpose

Manual-only gate for the first real provider smoke.

Default CI behavior remains fail-closed:

- network_call_attempted=false
- production_llm_call_attempted=false
- no OpenAI/Anthropic call in CI
- no deployment
- no release tag

## Required guard

Run before any real provider call:

    python scripts/ironclad_manual_provider_smoke_guard.py --json

The guard must report READY before any manual real provider smoke.

Required environment variables:

    $env:IRONCLAD_EXECUTOR_MODE="production_llm_enabled"
    $env:IRONCLAD_ALLOW_REAL_LLM="1"
    $env:IRONCLAD_EXECUTOR_DRY_RUN="0"
    $env:IRONCLAD_MANUAL_PROVIDER_SMOKE_CONFIRM="I_UNDERSTAND_THIS_MAY_CALL_A_PAID_PROVIDER"

Provider API key must be present, but must never be printed or committed:

    $env:OPENAI_API_KEY="<redacted>"

## Provider and model allowlist

Default allowed provider: openai

Default allowed models:

- gpt-4o-mini
- gpt-4.1-mini

Optional explicit allowlist:

    $env:IRONCLAD_ALLOWED_PROVIDERS="openai"
    $env:IRONCLAD_ALLOWED_MODELS="gpt-4o-mini,gpt-4.1-mini"
    $env:OPENAI_MODEL="gpt-4o-mini"

## Budget guard

Default maximums:

- IRONCLAD_REAL_SMOKE_MAX_TOKENS=128
- IRONCLAD_REAL_SMOKE_MAX_COST_CENTS=5

Optional stricter maximums:

    $env:IRONCLAD_REAL_SMOKE_MAX_TOKENS="64"
    $env:IRONCLAD_REAL_SMOKE_MAX_COST_CENTS="3"

Values above the default maximums must block the smoke.

## Manual smoke sequence

1. Confirm local git state is clean.

    git status

2. Run the guard.

    python scripts/ironclad_manual_provider_smoke_guard.py --json

3. Confirm status=READY and manual_provider_smoke_ready=true.

4. Only then run the manual real executor smoke.

    python scripts/ironclad_manual_real_executor_smoke.py --json --execute

5. Confirm redaction fields:

- api_key_value_exposed=false
- prompt_value_exposed=false
- output_text_value_exposed=false

## Rollback / cleanup

Clear manual smoke environment variables after the run:

    Remove-Item Env:\IRONCLAD_EXECUTOR_MODE -ErrorAction SilentlyContinue
    Remove-Item Env:\IRONCLAD_ALLOW_REAL_LLM -ErrorAction SilentlyContinue
    Remove-Item Env:\IRONCLAD_EXECUTOR_DRY_RUN -ErrorAction SilentlyContinue
    Remove-Item Env:\IRONCLAD_MANUAL_PROVIDER_SMOKE_CONFIRM -ErrorAction SilentlyContinue
    Remove-Item Env:\IRONCLAD_ALLOWED_PROVIDERS -ErrorAction SilentlyContinue
    Remove-Item Env:\IRONCLAD_ALLOWED_MODELS -ErrorAction SilentlyContinue
    Remove-Item Env:\IRONCLAD_REAL_SMOKE_MAX_TOKENS -ErrorAction SilentlyContinue
    Remove-Item Env:\IRONCLAD_REAL_SMOKE_MAX_COST_CENTS -ErrorAction SilentlyContinue
    Remove-Item Env:\OPENAI_MODEL -ErrorAction SilentlyContinue

Do not clear the provider API key unless the operator intentionally wants to remove it from the shell.

## Non-claims

This runbook does not make CI call OpenAI or Anthropic.

This runbook does not deploy anything.

This runbook does not create a release tag.

This runbook does not claim production deployment readiness.
