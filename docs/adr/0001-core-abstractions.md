# ADR 0001: Task, Object, and Resource are the minimal core

Status: Accepted

Mosaic's minimal portable model consists of Task, Object, and Resource. Actor, Stream, and Collective are extensions rather than hidden special cases.

This keeps the core execution semantics small enough to reason about retries, lineage, and placement while allowing stateful/streaming behavior to add stronger guarantees explicitly.
