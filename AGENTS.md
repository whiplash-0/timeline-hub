# Project Overview

This repository develops a personal Telegram bot that acts as a storage interface over S3-compatible object storage.

The system is used primarily by a single user, but the architecture should remain naturally extensible to multiple users (e.g. per-chat logic, allowlists), without overbuilding multi-user features.

This is not a traditional “backup” system. S3 is the source of truth, and stored objects may exist only there.

## Core purpose

The bot allows storing, retrieving, and modifying personal data via Telegram. Current domains include:

- Music
- Clips
- Memories

These domains may have different metadata and storage rules, so the design should allow domain-specific handling rather than forcing a single rigid schema.

## Storage model

Storage is prefix/group based.

A central concept is “season”:
- year + season number (1..5)

Additional metadata (e.g. scope, sub-season) may be used depending on the domain.

Deduplication is expected but defined at the domain level.

## Interaction model

The primary interface is Telegram menu buttons.

## Design intent

The project is a personal storage system with:

- S3 as durable storage
- Telegram as the control interface
- Vlear separation between infrastructure and domain logic
- Flexibility in how different media types are modeled

# Repository Agent Rules

## Tooling

- Use `uv` for dependency management and running commands.
- Pin exact versions when adding dependencies.
- Install the development environment with `uv sync --dev`.
- Repository uses a `src/` layout; tests must import the installed package.
- Do not modify `sys.path` in tests or use pytest/pythonpath hacks.
- Run tests with `uv run pytest`.

## Code Style

- Use modern Python 3.13+ style. Treat Python 3.13 as the baseline.
- Prefer current language features over legacy compatibility patterns.
- Do not introduce backward-compatibility constructs unless explicitly required.
- Prefer explicit code over clever abstractions.
- Use single quotes for normal strings; use triple double quotes for triple strings.
- Use Google-style docstrings.

Exception documentation:

- If a function intentionally raises exceptions as part of its contract, document them in a `Raises:` section.
- Only document exceptions that callers are expected to handle or that represent meaningful API behavior.
- Do not document incidental internal exceptions from underlying libraries unless they are part of the method's intended behavior.

## Operating Assumptions

Audience and scale:

- Primary user: repository owner; possibly a few trusted users.
- Traffic: low and mostly sequential.
- Developer time is the most constrained resource.

Design principles:

- Prefer simplicity and explicit assumptions over defensive completeness.
- Fail fast on unhandled exceptions so cloud restart/alerts surface issues.
- Soft or unbounded buffering is acceptable when aligned with personal usage.
- Handlers may orchestrate logic pragmatically; avoid premature module splitting.
- Logs should be concise and human-readable (cloud platform adds timestamps).
- Optimize for common paths and maintainability over exhaustive edge-case guards.

Refactor triggers:

Structure or guard rails should be added only after clear pain signals:

- repeated production failures
- difficult debugging
- increasing message/user volume
- handlers becoming hard to modify safely
- memory or runtime limits reached

Accepted risks:

- Some edge cases may remain intentionally unhandled.
- Resource bounds and stronger isolation are introduced after incidents, not preemptively.
- Maintenance speed and clarity take priority over defensive completeness.

### Agent behavior

Agents should:

- Avoid proposing large architectural changes for hypothetical scale.
- Treat some tradeoffs as intentional when they reduce maintenance cost.
- Prefer incremental improvements preserving current behavior and simplicity.
- Suggest next steps only when naturally useful; keep them concise and optional.

### Plan mode

When working in plan/spec mode:

- Prefer asking clarifying questions over making assumptions.
- If the task is ambiguous, ask multiple questions before proposing a plan.
- Use numbered questions.

### Recommendation policy

Default to the smallest change that solves today's problem.

Classify recommendations:

- now — required due to active impact
- later — optional until a trigger appears

### Testing expectations

- Cover critical paths and previously observed regressions.
- Exhaustive edge-case testing is not required by default.
- Prefer fast unit tests; add integration tests only for high-risk flows.

## Code Review Expectations

Reviews should evaluate more than correctness.

### Architecture

- Ensure abstraction boundaries match intended layers.
- Infrastructure code must remain generic and domain-independent.
- Avoid unnecessary layers, indirection, or premature abstractions.
- Prefer small explicit modules over framework-like structures.

### Modern Python

- Prefer modern Python 3.13 idioms.
- Avoid legacy compatibility constructs.
- Favor readable control flow over clever patterns.

### API design

- Public APIs should remain minimal and stable.
- Keep helper methods private unless external use is clearly justified.
- Method naming should reflect the abstraction level (infra vs domain).
- Avoid expanding the public surface without clear benefit.

### Internal `__init__.py` policy

For internal packages prefer empty `__init__.py`.

Rules:

- Do not create package-level APIs for internal packages unless requested.
- Do not re-export symbols just for convenience.

Preferred:

from general_bot.infra.tasks import TaskScheduler
from general_bot.infra.tasks import TaskSupervisor
from general_bot.infra.s3 import S3Client

Avoid:

from general_bot.infra import TaskScheduler

Use re-exports only when intentionally defining a stable package API.

### Maintainability

- Implementations should remain straightforward and easy to modify.
- Avoid unnecessary configuration or genericity.
- Code should remain understandable after long gaps.

### Reliability

- Consider failure modes and resource lifecycles.
- Ensure cleanup exists for files, streams, or clients.
- Prefer fail-fast behavior over silent error masking.

### Naming

- Names should match abstraction level and responsibility.
- Infrastructure code should avoid domain semantics.

### Simplicity rule

Even if no bugs exist, verify:

- the solution is the simplest clear implementation
- abstractions are justified
- the public API can be smaller or clearer

Review feedback should be grouped as:

- critical issues
- important improvements
- optional polish

## Commit Messages

Use Conventional Commits.

Format:

type(scope): short description

Optional body explaining the reasoning.

Example:

feat(infra): add fail-fast detached task supervision

Introduce `TaskSupervisor` for detached asyncio tasks with centralized
exception handling and a one-shot failure hook.

### Types

Allowed types:

- feat — new capability or user-visible behavior
- fix — bug fix
- refactor — internal restructuring without behavior change
- perf — performance improvement without behavior change
- test — tests added or updated
- docs — documentation-only changes
- chore — repository maintenance, tooling, or runtime requirement changes

Choose the type that reflects the **intent of the change**, not the file modified.  
Do not invent new types.

Examples:

- chore: update `.gitignore`
- chore(deps): bump `httpx` to 0.28.0
- chore!: bump python to 3.14

### Breaking changes

Agents must evaluate whether a change is backward-incompatible.

Typical breaking changes:

- renamed or removed environment variables
- renamed or removed settings fields
- changed configuration formats or required values
- changed public APIs
- changed CLI flags or behavior
- changed persisted data formats or schemas
- changed runtime requirements (e.g. minimum Python version)

Breaking commits must use:

type!: description  
type(scope)!: description

and include a footer:

BREAKING CHANGE: describe the migration required.

Example:

feat!: add superuser-aware shutdown notifications

BREAKING CHANGE: replace `USER_ALLOWLIST` with `SUPERUSER_IDS` and `USER_IDS`.

Configuration or settings contract changes should be assumed breaking
unless proven otherwise.

### Subject rules

Subjects must:

- be lowercase
- use imperative mood
- be ≤72 characters
- describe the system-level behavior change

Avoid vague subjects such as:

- "update rules"
- "improve system"

Prefer concise, action-oriented phrasing:

- use "bump" for version updates (e.g. python, dependencies)
- avoid overly abstract or policy-like wording

Implementation details should appear in the commit body.

### Referencing code entities

Wrap file names, modules, classes, and functions in backticks.

Example:

Rename `MessageBuffer` to `ChatMessageBuffer`.

### Shell safety for commit messages

Backticks trigger command substitution inside double quotes.

Incorrect:

git commit -m "Refactor `TaskScheduler` API"

Preferred:

git commit -m 'refactor(services): make task scheduler key-agnostic'

Safest method for multiline commits:

git commit -F - <<'EOF'
refactor(services): make task scheduler key-agnostic

Replace `TaskScheduler` user-coupled API with a generic `Hashable` key.
EOF

Agents must:

- avoid backticks inside double-quoted commit messages
- use single quotes or heredocs when backticks appear

### Referencing canonical repository files

When a commit primarily modifies a well-known document such as:

- `AGENTS.md`
- `README.md`
- `CHANGELOG.md`

mention it directly in the subject when helpful.

Example:

docs: expand `AGENTS.md` commit guidelines

### Commit body guidance

Add a body when reasoning is important, for example when:

- multiple subsystems are affected
- a refactor changes conceptual structure
- a rename clarifies an abstraction
- the reason is not obvious from the subject

The body should explain **why**, not list the diff.

### Scope discipline

Scopes should represent stable architectural subsystems.

Valid scopes:

- app — runtime bootstrap and wiring
- handlers — Telegram routing and handlers
- services — application services/state containers
- infra — shared infrastructure utilities
- settings — configuration loading and validation
- deps — dependency updates

Avoid choosing scope based only on a single touched file.

### Root-level files

Commits affecting repository-level files usually omit scope.

Examples:

docs: update `README.md`  
chore: update `.gitignore`

Use `deps` only for dependency changes.

### Generic infrastructure

Generic reusable modules belong to the `infra` scope.

Example:

feat(infra): add fail-fast task supervisor

### When scope is unclear

If a change does not clearly belong to a subsystem, omit the scope.

Example:

feat: add task supervisor utility

### Commit coherence

Commits should remain conceptually coherent by subsystem and intent.

Do not combine unrelated changes in a single commit.  
If unrelated local modifications exist, commit only the files relevant to the requested change.
