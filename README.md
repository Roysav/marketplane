# marketplane

A trading platform where strategy agents declare intent as records, and controllers handle execution.

Agents write high-level objects — a spread trader config, a position target. Controllers watch those records and translate them into orders, risk checks, exchange calls, and status updates. No agent touches the exchange directly.

Tradespaces isolate strategies with independent capital allocations. A controller can shift funds between them based on performance, making tradespaces the natural unit for running and scaling strategy experiments.
