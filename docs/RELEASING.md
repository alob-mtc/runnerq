# Releasing runner_q and runner_q_redis

Both crates are published to [crates.io](https://crates.io) from this repo. **Versions can change independently.** The tag you push decides which crate(s) get published.

## Tag format → what gets published

| Tag | Publishes |
|-----|-----------|
| `v0.6.5` (plain) | **Both** — `runner_q` then `runner_q_redis` |
| `runner_q-v0.6.5` | **runner_q only** |
| `runner_q_redis-v0.1.2` | **runner_q_redis only** |

## Prerequisites

- Push access to the repo and permission to create tags.
- [crates.io](https://crates.io) auth configured for the repo (e.g. GitHub Actions with `crates-io-auth-action`).

## Draft release notes (Release Drafter)

On every push to `main`, [Release Drafter](https://github.com/release-drafter/release-drafter) updates **three** draft releases:

| Draft | Tag it will use when you publish |
|-------|-----------------------------------|
| **v0.6.6** (combined) | `v0.6.6` |
| **runner_q v0.6.6** | `runner_q-v0.6.6` |
| **runner_q_redis v0.1.1** | `runner_q_redis-v0.1.1` |

Each draft shows PRs merged since the last tag of that kind. When you’re ready to release, pick the draft that matches how you’re releasing (one crate or both), publish it so GitHub creates the tag, then the publish workflow will run and push to crates.io (or create the tag yourself and paste the draft notes into the release).

## Release flow

### Release one crate only

1. **Bump the version** in that crate’s `Cargo.toml`:
   - `runner_q` only → root `Cargo.toml` → `version`
   - `runner_q_redis` only → `runner_q_redis/Cargo.toml` → `version`

2. **Commit and push** the version bump.

3. **Create and push the tag** for that crate:
   ```bash
   # Release runner_q only (e.g. 0.6.5)
   git tag runner_q-v0.6.5
   git push origin runner_q-v0.6.5
   ```
   or
   ```bash
   # Release runner_q_redis only (e.g. 0.1.2)
   git tag runner_q_redis-v0.1.2
   git push origin runner_q_redis-v0.1.2
   ```

4. The workflow (`.github/workflows/release.yml`) publishes only the matching crate.

### Release both together

1. **Bump versions** in both `Cargo.toml` files.
   - To keep versions aligned, set both to the same value (e.g. `0.6.5`).
   - You can also bump each crate to its own next version if you version independently.

2. **Commit and push.**

3. **Tag with a plain `v` tag:**
   ```bash
   git tag v0.6.5
   git push origin v0.6.5
   ```

4. The workflow publishes `runner_q` first, then `runner_q_redis`.

## Version alignment (optional)

- You can keep **runner_q** and **runner_q_redis** on the same major/minor (e.g. both `0.6.x`) so `runner_q_redis = "0.6"` works with the published `runner_q`.
- Or version them independently (e.g. `runner_q` at `0.6.x`, `runner_q_redis` at `0.1.x`). When publishing `runner_q_redis`, the root `Cargo.toml` version of `runner_q` is what gets written as the dependency in the published crate — so that version of `runner_q` must already exist on crates.io.

## Troubleshooting

- **Publish fails for runner_q_redis**  
  The published dependency on `runner_q` is taken from the root `Cargo.toml`. Ensure that version of `runner_q` is already published to crates.io (e.g. release `runner_q` first with a `runner_q-v*` or `v*` tag).

- **Already published**  
  If the tag is re-used or the workflow re-runs, `cargo publish` will fail for a crate that’s already at that version. Bump the version, commit, and use a new tag.
