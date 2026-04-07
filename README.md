# Timeline Hub

Deterministic Telegram bot for storing, normalizing, and retrieving video clips through a fixed-layout Telegram UI and a structured S3-backed archive.

## Overview

`timeline-hub` is built around a narrow but intentionally opinionated workflow:

- users send video clips in a private chat
- the bot briefly buffers incoming messages, reconstructs Telegram media albums, and presents a single action menu
- clips can then be normalized with `ffmpeg`, stored into a structured archive, or fetched back through a deterministic selection flow

Stored clips are organized by `year -> season -> universe -> sub-season -> scope`. Each logical clip group lives under a single S3 prefix and is indexed by a `manifest.json` file. Fetching is driven by that manifest; storing uploads new clip objects first, then commits the updated manifest, with rollback if the manifest step fails.

The runtime itself is small: `aiogram` handles Telegram updates, a compact service container exposes domain operations, and the infrastructure layer provides generic S3 and async task primitives. The distinctive part of the project is not framework complexity, but the strictness of the interaction model, storage semantics, and failure behavior.

## Key ideas

- The Telegram UI is treated as a fixed spatial interface rather than a dynamic list. Inline-keyboard messages always render as exactly three text lines and three button rows.
- Menus use deterministic placement instead of left-to-right packing. Option grids follow a top-right-first snake layout, and unavailable options become inert dummy buttons instead of collapsing the layout.
- Store and Get/Pull flows deliberately share the same structural layouts. The UI can differ from the domain model at render time, while storage and parsing still use domain enums as the source of truth.
- Clip storage is manifest-backed rather than inferred from object listing alone. That gives strict ordering, subgroup discovery, validation, and clip-group-local deduplication by hash or existing clip id.
- Background work is fail-fast. Detached task failures are logged, routed through a one-shot failure hook, and lead to superuser notification plus polling shutdown instead of silent degradation.
- Tests encode these choices directly. They verify keyboard slot placement, message padding, stale-menu handling, manifest shape, deduplication behavior, and async task semantics.

## Architecture

The project is split into small layers with narrow responsibilities.

- `app` is the composition root. It loads immutable settings, configures logging, opens the Telegram bot and S3 client, installs allowlist middleware, injects the service container into the dispatcher, and wires handler or background-task failures to superuser notification and polling shutdown.
- `handlers` own Telegram-facing behavior. `clips_store.py` manages buffered uploads, action selection, normalization, and storage menus. `clips_retrieve.py` drives the Get/Pull flow from entry menu to batched clip delivery. `clips_common.py` holds shared FSM state, menu parsing, text formatting, and fixed-layout keyboard primitives.
- `services` provide the domain-facing operations used by handlers. `ClipStore` maps logical clip groups to S3 objects and manifests, `ChatMessageBuffer` batches incoming messages per chat and reconstructs media groups, and `Services` is an explicit container rather than implicit global state.
- `infra` contains reusable plumbing. `S3Client` is a generic async wrapper over S3-compatible storage, and `tasks.py` provides a detached task supervisor plus a per-key debouncing scheduler.
- `settings` loads configuration from `.env` with `pydantic-settings`, selects `BOT_TOKEN_DEV` when running with `--dev`, and automatically folds `SUPERUSER_IDS` into the main allowlist.
- `domain.py` contains media-specific logic that does not belong in Telegram handlers. At the moment this is two-pass audio normalization via `ffmpeg loudnorm`, using temporary files because MP4 output must remain seekable.

At runtime, Telegram updates enter handlers, handlers translate them into domain-level operations, services execute those operations, and infrastructure provides the underlying storage and task primitives. The app layer stays responsible for wiring, lifecycle, and failure policy.

## Core design concepts

- Deterministic menu geometry. Messages with inline keyboards are rendered as either `padding, padding, content` or `context, padding, prompt`, and keyboards always occupy three rows. This removes layout shift and lets the user rely on position instead of rescanning the screen.
- Snake-ordered option grids. The first two keyboard rows are filled from the top-right corner in a snake pattern. Newer or more relevant options naturally land in the easiest-to-reach positions without turning the menu into ad hoc per-step logic.
- UI as a projection of the domain. `Season`, `Universe`, `SubSeason`, and `Scope` remain authoritative domain values, while the handler layer is free to reorder, group, or dummy-fill them for usability. That keeps storage and parsing stable while allowing a highly opinionated Telegram UI.
- Manifest-backed clip groups. Each group under `clips/<year>-<season>-<universe>/` stores clip objects plus a `manifest.json` index. The manifest is validated on load, defines subgroup membership and order, and acts as the commit boundary for store operations.
- Buffered ingestion before user choice. Incoming private-chat messages are collected per chat and released after a short debounce. The bot can then present one menu for a burst of forwarded clips and preserve Telegram media-group structure when storing or resending.
- Fail-fast async orchestration. Detached tasks are supervised centrally. Debounced jobs can be rescheduled by key, but once real work starts it is shielded from cancellation, and unexpected failures trigger a clear shutdown path.

## Project structure

src/
  timeline_hub/
    app.py
    settings.py
    domain.py
    handlers/
      core.py
      clips_common.py
      clips_retrieve.py
      clips_store.py
      router.py
    services/
      clip_store.py
      message_buffer.py
      container.py
    infra/
      s3.py
      tasks.py
tests/
  test_handlers.py
  test_clip_store.py
  test_s3.py
  test_tasks.py
  test_app.py
  test_chat_message_buffer.py

## Installation

Python 3.14 is required.

uv sync --group dev

Create `.env` from `.env.example`. The bot operates only in private chats and requires an allowlist configuration.

Required settings:

- BOT_TOKEN
- SUPERUSER_IDS
- S3__ENDPOINT_URL
- S3__REGION
- S3__BUCKET
- S3__ACCESS_KEY_ID
- S3__SECRET_ACCESS_KEY

Optional settings used by the code include:

- BOT_TOKEN_DEV for `--dev` runs
- USER_IDS for additional allowlisted users beyond `SUPERUSER_IDS`

`ffmpeg` must also be available on PATH; it is used for audio normalization and clip processing.

## Usage

Run the bot through the package entry point:

uv run python -m timeline_hub

For the development bot token:

uv run python -m timeline_hub --dev

The main entry point is `src/timeline_hub/__main__.py`, which delegates to `timeline_hub.app.run()`.

## Development

Local checks cover the same quality gates as CI, and `pre-commit` is the main gate before changes are committed.

uv run pytest
uv run ruff check --fix
uv run ruff format
uv run pyright
uv run pre-commit run --all-files

In this repository, `pre-commit` runs EOF/TOML checks, Ruff linting and formatting, and Pyright.

## CI

GitHub Actions runs on Python 3.14, installs dependencies with `uv sync --group dev`, and executes:

- `uv run ruff check`
- `uv run pyright`
- `uv run pytest`

These checks run in separate jobs so lint, typing, and test failures stay easy to identify while remaining aligned with the local development workflow.

## Design principles

- Prefer deterministic behavior over compact or adaptive UI.
- Keep abstractions small and explicit; most modules have one job.
- Separate Telegram presentation from domain and storage semantics.
- Treat object storage as infrastructure and the clip archive as a domain service.
- Fail visibly when background work breaks instead of masking the problem.
- Encode invariants in tests, especially for layout and ordering rules.

## Notes / constraints

- Clip flows are limited to private chats and gated by an allowlist middleware.
- FSM state uses in-memory storage, so conversational state is local to the running process.
- `ClipStore` assumes sequential writes per logical clip group; concurrent writers to the same group are not supported.
- Fetching is batched, but ordering is always driven by manifest order, not by object listing order.

## Extensibility

The code is set up to grow by composition rather than framework-building. New Telegram flows can reuse the shared menu primitives and FSM helpers in `clips_common.py`, while new storage-oriented behavior can build on the existing `ClipStore` and `S3Client` boundary without leaking Telegram concerns into infrastructure code.
