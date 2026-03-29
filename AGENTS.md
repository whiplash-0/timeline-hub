# Project Overview

## Telegram UI invariants

These rules apply only to messages that include inline keyboards. Plain text messages must remain unmodified and must not be padded or height-normalized.

Every message with buttons must render:
- exactly 3 text lines
- exactly 3 rows of buttons

The UI is intentionally fixed-size and structurally predictable:
- button positions follow stable patterns
- the same conceptual actions stay in the same areas
- missing options do not collapse layout; they are replaced structurally

## Intake action invariants

All intake actions operate on the buffered chat messages.

Actions are classified into:
- single-shot actions
- interactive actions

Single-shot actions:
- flush the buffer immediately on entry
- validate after flush
- do not restore the buffer on failure
- intentionally keep the UI stateless
- require the user to resend clips after rejection if needed

Interactive actions:
- enter a multi-step stateful flow only after successful entry validation
- do not flush the buffer on entry after validation succeeds
- use buffer versioning for consistency
- invalidate interaction if the buffer changes
- flush only on final execution

Failed entry validation is not part of the interactive flow. It is a stateless rejection and may intentionally flush the buffer to keep the UI simple.

## Validation invariants

Validation must complete before execution.

Rules:
- no downloads before validation completes
- no `store()` calls before validation completes
- no partial execution

Validation must be:
- deterministic
- global across the full input
- consistent with Get/Pull logic where applicable

Validation failures should:
- return a single generic error message
- avoid partial processing
- avoid side effects unless the stateless rejection model intentionally flushes the buffer

## Ordering invariants

Message order is authoritative.

Rules:
- preserve original message order unless the user explicitly changes it
- preserve relative order in transformations such as Route batching
- keep `store()` input order identical to original message order

Reorder defines an explicit new order and final output must follow it exactly.

## Interaction invariants

Interactive flows must remain version-safe.

Rules:
- all interactions depend on buffer version
- version mismatch invalidates the interaction
- invalidation removes buttons and shows `Selection is no longer available`

Back behavior:
- resets only local interaction state
- does not partially revert state
- returns to the main action menu

## Media handling invariants

Treat Telegram media groups as transport detail only.

Rules:
- flatten all input video messages
- ignore original media-group boundaries
- reconstruct dense output media groups
- respect Telegram’s max 10 items per media group
- operate on logical clip sequences, not Telegram grouping

## Text layout

Only real text lines count as content. Padding lines exist only for layout stability.

Allowed layouts:

Single-content messages:
- real content on line 3
- lines 1 and 2 are padding

Context + prompt messages:
- context on line 1
- line 2 is padding
- prompt on line 3

Padding must use one consistent width-based mechanism.
Do not use:
- manual spacing
- empty lines
- alternate padding tricks

## Selected formatting

Selection state must be visually structured.

Rules:
- `Selected:` is plain text
- each selected value is emphasized individually
- separators are plain
- do not emphasize the whole concatenated string

## Button layout invariants

All inline keyboards must always have exactly 3 rows.

Buttons follow a fixed spatial grid:
- top + middle rows = selectable options
- bottom row = navigation or terminal action

If Back exists:
- it occupies the entire bottom row
- its position never changes

### Option grid layouts

The first two rows are a deterministic option grid.

Two layouts are allowed:
- snake layout
- columnar right-to-left layout

Use the layout already established for the specific flow. Do not switch an existing flow from one layout to the other unless the task explicitly requires that UI change.

### Snake layout

Snake rules:
- start at the top-right corner
- go down
- move left
- go up
- continue alternating while moving left

If the number of option slots is odd:
- the first row contains one fewer slot than the second row

Do not place options sequentially left-to-right.

### Columnar right-to-left layout

Columnar rules:
- group options in input-order top/bottom pairs
- treat each pair as one vertical column
- render columns from right to left
- preserve the fixed 3-row keyboard shape

Example with `[1,2,3,4,5,6]`:
- top row = `[5,3,1]`
- middle row = `[6,4,2]`

## Fixed layout across flows

Menus must not change shape based on data availability.

Rules:
- define a full set of possible option slots
- render available options normally
- render unavailable options as structural dummy placeholders
- never collapse layout because data is missing
- never insert placeholders if enough real options exist

## Dummy buttons

Dummy buttons are structural only.

Rules:
- preserve layout height and option positions
- remain inert
- do not affect logic, parsing, or state transitions
- do not visually compete with real actions

## UI/domain separation

UI is a projection of domain state.

Rules:
- domain enums and values remain authoritative
- UI may reorder or group values for usability
- UI transformations must not affect storage, parsing, or business logic

# Repository Agent Rules

## Tooling

Environment and workflow:
- Use `uv` for dependency management and command execution.
- Pin exact versions when adding dependencies.
- Install the dev environment with `uv sync --dev`.
- The repository uses `src/`; tests must import the installed package.
- Do not modify `sys.path` in tests or use pytest/pythonpath hacks.

Testing:
- Run tests with `uv run pytest`.

Pre-commit:
- Pre-commit is the final enforcement gate.
- Do not bypass hooks.
- If hooks fail, fix the issues before committing.
- Ruff and Pyright must pass through the normal workflow.

## Code style

- Target modern Python as defined by `pyproject.toml`.
- Prefer current language features over legacy compatibility patterns.
- Prefer explicit code over clever abstractions.
- Do not rely on private attributes or methods unless explicitly required or there is no viable public alternative.
- Use absolute imports only.
- Import paths should start from the top-level package.
- Document intentional contract-level exceptions in `Raises:` sections.
- Do not document incidental internal exceptions unless they are part of the intended API behavior.

Type checking:
- Do not distort architecture just to satisfy static analysis.
- If a library relies on dynamic runtime behavior that static analysis cannot model, prefer a narrow, well-commented suppression over architectural duplication or private API usage.

## Operating assumptions

Audience and scale:
- primary user is the repository owner, possibly a few trusted users
- traffic is low and mostly sequential
- developer time is the most constrained resource

Design principles:
- prefer simplicity and explicit assumptions over defensive completeness
- fail fast on unhandled exceptions
- soft or unbounded buffering is acceptable when aligned with personal usage
- handlers may orchestrate logic pragmatically
- keep logs concise and human-readable
- optimize for common paths and maintainability over exhaustive guards

Refactor triggers:
- repeated production failures
- difficult debugging
- increasing message or user volume
- handlers becoming hard to modify safely
- memory or runtime limits reached

Accepted risks:
- some edge cases may remain intentionally unhandled
- stronger isolation or hard limits can be added after real incidents
- maintenance speed and clarity take priority over defensive completeness

## Review expectations

Reviews should evaluate:
- abstraction boundaries
- architecture fit by layer
- simplicity
- maintainability
- failure modes and cleanup
- API surface
- naming quality
- modern Python usage

Infrastructure code must remain generic and domain-independent.

For internal packages:
- prefer empty `__init__.py`
- do not create package-level APIs unless explicitly requested
- do not re-export internal symbols for convenience

Preferred:
- `from general_bot.infra.tasks import TaskScheduler`
- `from general_bot.infra.tasks import TaskSupervisor`
- `from general_bot.infra.s3 import S3Client`

Avoid:
- `from general_bot.infra import TaskScheduler`

Review feedback should be grouped as:
- critical issues
- important improvements
- optional polish

## Commit messages

Use Conventional Commits.

Format:
- `type(scope): short description`

Core rule:
- describe the actual system change, not the tool, complaint, file operation, or mechanical diff

Allowed types:
- feat
- fix
- refactor
- perf
- test
- docs
- chore

Type guidance:
- `feat` = new capability or user-visible behavior
- `fix` = corrected runtime behavior
- `refactor` = restructuring without intended behavior change
- `docs` = documentation-only changes
- `chore` = tooling, formatting, dependencies, repo maintenance, runtime-version bumps

Breaking changes:
- use `type!:` or `type(scope)!:`
- include a `BREAKING CHANGE:` footer
- assume config, settings, env-var, public API, persisted-format, and runtime-version changes are breaking unless clearly proven otherwise

Subject rules:
- lowercase by default
- imperative mood
- ≤72 characters
- system-level wording
- avoid vague or low-signal subjects
- implementation details belong in the body, not the subject
- when updating `AGENTS.md`, name the affected agent rules or behavior explicitly
- avoid vague terms like `guidance`, `cleanup`, or `tweaks` when the change affects agent instructions or decision rules

User-facing concepts in subjects:
- prefer neutral lowercase phrasing by default
- do not preserve UI sentence casing unless needed for disambiguation
- avoid backticks in subjects unless they add clear value

Prefer:
- `refactor(handlers): rename fetch flows to get and pull`
- `docs: clarify agent commit scope rules`

Over:
- `refactor(handlers): rename \`Fetch\` flows to \`Get\` and \`Pull\``
- `docs: clarify commit scope guidance`

Backticks remain acceptable in bodies for precise code entities.

Tooling-related subjects:
- mention the tool only when the change is truly about adopting or configuring it
- otherwise describe the resulting system change

Code-entity references:
- wrap file names, modules, classes, and functions in backticks when referenced in prose or bodies

Shell safety:
- avoid backticks inside double-quoted commit messages
- prefer single quotes or heredocs when backticks appear

Commit bodies:
- body optional for tiny obvious changes
- short body preferred by default for non-trivial commits, especially:
  - wide-surface renames
  - behavior-preserving structural changes
  - broad refactors touching many files
  - multi-subsystem changes
  - changes where preserved semantics or guarantees matter

Bodies should:
- use normal sentence case
- explain why
- summarize scope and key guarantees
- avoid mechanically restating the diff

Good body topics:
- pure rename / no behavior change
- preserved semantics
- key invariants or guarantees
- scope boundaries

Scopes:
- use stable architectural subsystems
- valid scopes: `app`, `handlers`, `services`, `infra`, `settings`, `deps`
- omit scope when the change is cross-cutting or repository-level
- choose scope by system intent, not just touched file
- do not use a narrow scope when the change modifies contracts or behavior across multiple subsystems
- if handlers orchestration depends on new services/domain contracts, usually omit scope

Root-level files usually omit scope.

Commit coherence:
- keep commits conceptually coherent by subsystem and intent
- split unrelated changes
- do not mix substantial runtime logic changes with unrelated tooling or formatting if that hurts clarity
