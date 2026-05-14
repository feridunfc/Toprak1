
# Faz 5A — Decision Observability

## Goal
Make scheduler decisions explainable and inspectable in production.

## What this pack adds
- structured scheduler decision trace
- rejected worker reasons
- scored candidate breakdown
- selected score + selected reason
- observable capability + fairness + reservation dispatch path

## Why it matters
Without this layer, a production scheduler becomes extremely hard to debug:
- why was this worker selected?
- why were others rejected?
- what exact score won?
- was it capability or fairness that dominated the decision?
