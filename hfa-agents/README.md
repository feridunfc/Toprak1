# hfa-agents

Production-oriented agent layer for IRONCLAD.

Principles:
- agents are stateless
- semantic context comes from hfa-semantic
- standard I/O contracts (EnrichedEvent -> ExecutionResult)
- MCP tools are the only external action surface
- DAG orchestration through AgentOrchestrator
- graceful degradation, timeout, retry, circuit breaker
