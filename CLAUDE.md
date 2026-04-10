# General

> **Meta-rule**: Rules in this file include their rationale. Rules with rationales are followed more reliably and transfer better to novel situations.
- **Documentation is dual-use**: Agents start each session with no memory. All docs under `docs/` serve both humans and agents — include exact commands, paths, and rationales for non-obvious steps.
- **Do not be a yes-man**: Humans make bad decisions and forget to tell the whole picture. Ask user to clarify and use the data to improve your decisions.

# Codebase Stage & Work Modes

This codebase is in active evolution. Existing code is provisional — ideas may be sound but execution is rough. Do not preserve existing patterns out of respect for the diff. Preserve them only if they're actually correct.

Evolution is data-driven. The system captures correct raw data; the logic that processes it is what's being iterated on. Bugs and anomalies in production data are domain discovery signals — ask "what concept is missing?" before asking "what code do I patch?"

"Prefer minimal edits" means: minimize comprehension cost for session N+5, not line count in this diff. Every session starts cold. A wrong model re-discovered is worse than a large refactor done once.

**Work modes** (user sets at session start or switches mid-session):
- **Evolve** (~70%): evolve domain model and codebase toward correct modeling. Refactors and rewrites are welcome — including revolutionary ones. **Default mode.**
- **Analyze** (~15%): read logs/data, make plans, no code changes.
- **Meta** (~10%): improve interaction workflows.
- **Patch** (~5%): minimal diff for a specific problem. Never assume this mode.

**Design rules** (evolve mode):
- Domain objects over god services. If logic only needs one object's data, it belongs on that object.
- Make invalid states non-representable. If two values are meaningless without each other, they're one type. If a pipeline has stages, the stage outputs are types.
- Concepts are mapped into domain objects. Nouns are types. Verbs can be both methods and types.

**Cognitive phases in this project**: The system prompt defines Think → Design → Implement phases. In this project, Think phase is almost always data-driven — run queries against GTFS, read observations, check raw API responses. Both user assumptions and agent assumptions fail on contact with data. Let observations decide before proposing models.

# Project Rules and Guidelines

## Environment & Configuration
- **Git Protocol**: User intent is always partial; they edit files between turns. Never commit until triggered. Inspect actual state when committing.
- **Commit Prefix**: `agent:` for AGENTS.md, CLAUDE.md, `.agents/` changes (artifacts that exist because agents exist). `docs:` for system knowledge (consumed by humans and agents alike).

## Workflow & Planning
- **Hypothesis Protocol**: When investigating or brainstorming:
    1. **State multiple conflicting hypotheses.** "hypo: could be X, or Y, or Z — they predict different things." A single hypothesis is a conclusion in disguise. Multiple competing ones force useful discussion and let the user prune with domain context.
    2. **Don't auto-validate.** State hypotheses to the user first so they can prune with domain context. Then validate with data. Running off to verify for 10 minutes without stating what you're checking is waste.
    3. **Persist what survives.** Confirmed facts and instructive dead-ends go to `docs/domain-knowledge/` (see convention below). Session-scoped hypotheses go in the relevant plan doc or KDoc.
- **Review Mode**: Unreviewed changes are risky; over-proposing is cheap. For non-trivial changes: propose in text, read-only checks only, wait for approval. Surface everything relevant you find, not just what was asked.
- **Incremental Implementation**: Do not assume you know the full task scope. User can either forget to prompt the whole scope or omit it by purpose. Work on small sub-tasks, verify alignment after each.
- **Plan Fidelity**: During implementation, do not reduce the scope of the plan. If the plan says "do A," implement A — not a halfway compromise between A and something simpler. Plans represent completed thinking; silently delivering less discards that thinking without discussion. The failure mode is severe: the user discovers the gap days later when session context is gone, and must re-derive the plan's reasoning before they can even assess whether the reduction was justified. If A turns out to be genuinely wrong or infeasible during implementation, **stop and update the plan first** — don't quietly deliver partial work and call it done.
- **Scope Decisions Are Not Yours to Make Silently**: When you identify related work during planning, you have two acceptable options: (1) **Include it** — if excluding would break logical coherence (same code, same reasoning chain, enabled by the same change). This is the default. (2) **Ask** — "this is related but separable, should we park it?" Let the user decide. You do NOT get to unilaterally exclude work — not silently, not aloud. "That's separate work" is never a conclusion; it is at best the start of a question to the user.
- **Meta Feedback**: Workflow improvements are perishable. On "meta:" prefix, pause immediately. Propose instruction-file changes via Review Mode, apply after approval, then resume the original task. Always update project documentation, CLAUDE.md instead of memory files.

## Plans (`docs/plans/`)
Plans are handoff docs for keeping context between cold-start sessions. They are the primary continuity mechanism — write for the agent that picks this up in 2 hours, not for posterity.

- **Format is adhoc.** No template. Natural structure is problem → high-level approach → low-level details, but the shape follows the problem. Rejected alternatives and why are the highest-value content — they prevent the next session from re-deriving the same options.
- **Plans are high-level.** They must contain human-readable description of most non-trivial logic. For code references, prefer minimal references to the entry points of code that will help understand the logic or decision points.
- **Lifecycle**: Plans exist while work is in-flight. When the work is done, **delete the plan** and push its knowledge into persistent artifacts (Docstrings, AGENTS.md, test descriptions, doc files). Completed plans left in the workspace become stale context that misleads future sessions.
- **Index (`docs/plans/_index.md`)**: Tracks active plans (one-liner + status per file) and a **parking lot** for small ideas — the "we should do this, maybe later" observations that aren't worth a plan file yet. Update the index when creating or deleting plans, and when ideas come up during any session.
- **When to plan**: If you need to think before coding, the thinking goes in a plan. If you don't, just code. Mixing planning and implementation in the same pass compounds errors — for complex tasks, write the plan first, then implement.