# Stream Terragrunt inputs through a FIFO `*.auto.tfvars.json` instead of `TF_VAR_*`

- **Date:** 2026-06-09
- **Repo:** `/workspace/terragrunt`
- **Cross-reference:** OpenTofu source at `/sources/opentofu` (v1.13.0-dev)
- **Upstream context:** [gruntwork-io/terragrunt#4402](https://github.com/gruntwork-io/terragrunt/issues/4402)

## Problem

Terragrunt passes every value from its `inputs` map to OpenTofu/Terraform as a
`TF_VAR_<name>` environment variable (`SetTerragruntInputsAsEnvVars` →
`ToTerraformEnvVars`, `internal/runner/run/run.go`). The combined size of `argv`
+ `envp` handed to a child process is bounded by the kernel's `ARG_MAX`. With
many or large inputs this overflows and `fork`/`exec` fails with `E2BIG`, which
is the failure reported in #4402.

Two goals motivate the fix (both selected by the requester):

1. **Scale** — get input values out of the process environment so `ARG_MAX` is
   no longer a function of input size.
2. **Secrecy** — keep input *values* off both the environment (readable via
   `/proc/<pid>/environ`, `ps -E`, child processes) **and** the filesystem.

## Solution overview

On Unix, terragrunt stops writing input values into any child environment.
Instead, immediately before each child process is executed, it creates a
**named pipe (FIFO)** at `<dir>/.terragrunt-inputs.auto.tfvars.json` (mode
`0600`) and a background goroutine streams a JSON object of all inputs into the
pipe each time a reader opens it. OpenTofu/Terraform auto-loads
`*.auto.tfvars.json` files from its working directory, so it consumes the inputs
with no flags and no on-disk persistence. The pipe is removed when the child
exits.

On Windows (no POSIX FIFOs), the current `TF_VAR_*` behavior is retained
verbatim, build-tagged. This is a documented platform limitation: the `ARG_MAX`
fix and the off-env/off-disk guarantees do **not** apply on Windows.

### Why this works (feasibility, verified against `/sources/opentofu`)

`internal/command/meta_vars.go`:

- `addVarsFromDir` (line ~151) discovers auto-files with `os.ReadDir` +
  `isAutoVarFile(name)`. `isAutoVarFile` (`meta.go:490`) is a **pure suffix
  check** (`.auto.tfvars` / `.auto.tfvars.json`) — there is **no `IsRegular()`
  or file-mode check**, so a FIFO with that suffix is accepted. `os.ReadDir`
  includes dotfiles.
- `addVarsFromFile` (line ~166) reads via `os.ReadFile` → `os.Open` blocks until
  a writer opens the FIFO, then reads to EOF when the writer closes. FIFO-safe.
- `collectVariableValues` caches results in `m.inputVariableCache`
  (lines ~47, ~130), so the file is read **exactly once per tofu process**.
- The existing `.terragrunt-null-vars.auto.tfvars.json` (a real file terragrunt
  already drops in the working dir) is live proof that tofu reads a
  dot-prefixed `*.auto.tfvars.json` from the working dir.

FIFO open rendezvous: a reader blocked in `open(O_RDONLY)` increments the
kernel's reader count before parking, so the writer's
`open(O_WRONLY|O_NONBLOCK)` succeeds (no `ENXIO`) once tofu is waiting — the poll
model below converges without deadlock.

## Design decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **FIFO, all inputs, mode 0600, Unix only** | Off-env + off-disk; the file never holds bytes at rest. |
| D2 | **Non-blocking poll writer** | Deadlock-free and cleanly cancelable when a command never reads the file (`init`, `version`, arbitrary `exec`). |
| D3 | **Per-exec lifetime** | One FIFO + goroutine per child process, set up right before exec, torn down right after — mirrors the existing null-vars file handling; auto-init recursion is handled for free. |
| D4 | **Nulls folded into the FIFO** | Removes the separate on-disk null-vars file (Unix); one mechanism; nulls stay off disk too. |
| D5 | **User-set `TF_VAR_x` still wins** | An input whose `TF_VAR_<name>` is already present in the caller's env is omitted from the FIFO, so tofu reads the user's env value at its natural lowest precedence — preserves today's "don't override" behavior. |
| D6 | **Fail closed** | Any error creating/serving the FIFO aborts the run; never a silent fallback to env or a real file. |
| D7 | **Scope: all invocations incl. `exec`** | No input values in any child environment. Caveat: an arbitrary `terragrunt exec -- <prog>` only sees inputs if `<prog>` reads `*.auto.tfvars.json` (i.e. is tofu/terraform). |
| D8 | **Windows: keep `TF_VAR_*`** | POSIX FIFOs are unavailable; retain current behavior, build-tagged and documented. |

### Behavior delta: variable precedence (must be documented in changelog/docs)

OpenTofu precedence (low → high): `TF_VAR_` env → `terraform.tfvars[.json]` →
`*.auto.tfvars[.json]` in lexical name order → `-var`/`-var-file`. Moving inputs
from env to an auto-tfvars file **raises their precedence**:

- The dot-prefixed name `.terragrunt-inputs.auto.tfvars.json` sorts before
  typical user auto-files, so a user's own `prod.auto.tfvars` **still overrides**
  terragrunt inputs (preserved).
- However, terragrunt inputs now **override** a module's `terraform.tfvars`,
  where previously (env precedence) `terraform.tfvars` won. Narrow but real.

There is no way to reproduce env-level (lowest) precedence with a file; this
delta is inherent to the chosen mechanism and is accepted.

## Components and changes

### New: `internal/runner/run/inputs_tfvars.go` (`//go:build !windows`)

- `const InputsTFVarsFile = ".terragrunt-inputs.auto.tfvars.json"`
- `func setupTerragruntInputs(ctx, l, dir string, opts *Options, cfg *runcfg.RunConfig) (cleanup func(), err error)`
  - Build the input map: every entry of `cfg.Inputs` **except** those whose
    `TF_VAR_<name>` already exists in `opts.Env` (D5). Includes `nil` values
    (D4). Encode the whole map with `json.Marshal(map[string]any{...})` — exactly
    as `setTerragruntNullValuesRunCfg` does today for the null subset (a tfvars
    JSON file is one JSON object of `name → value`; do **not** use the per-env-var
    `util.AsTerraformEnvVarJSONValue` string encoding here).
  - If the resulting map is empty, return a no-op cleanup and create no FIFO.
  - Remove any stale node at the path, then `unix.Mkfifo(path, 0o600)`. On error,
    return it (D6 fail-closed).
  - Start a goroutine bound to a derived `ctx`: loop `open(O_WRONLY|O_NONBLOCK)`;
    on `ENXIO`/`ErrWouldBlock` sleep (~10ms) and re-check `ctx`; on success,
    write all JSON bytes then `Close()`; treat `EPIPE` as debug, non-fatal;
    continue until `ctx` is cancelled.
  - `cleanup`: cancel the goroutine's context, unblock/close any in-progress
    open or write, wait for the goroutine to exit, then `os.Remove(path)`
    (ignore `ErrNotExist`).

### New: `internal/runner/run/inputs_tfvars_windows.go` (`//go:build windows`)

- Same `setupTerragruntInputs` signature. Body replicates today's behavior:
  merge `ToTerraformEnvVars(cfg.Inputs)` into `opts.Env` (skip already-set keys),
  write null-valued inputs to `NullTFVarsFile` in `dir`, and return a cleanup
  that removes that file. No FIFO.

### Modified: `internal/runner/run/run.go`

- `runTerragruntWithConfig`: remove the `SetTerragruntInputsAsEnvVars` call
  (line ~250) and the null-vars block (lines ~264-276). Insert, at the point
  where the null-vars file was set up (after init handling, before
  `RunActionWithHooks`):
  ```go
  cleanupInputs, err := setupTerragruntInputs(ctx, l, opts.WorkingDir, opts, cfg)
  if err != nil { return err }
  defer cleanupInputs()
  ```
- The platform difference lives **entirely inside the build-tagged
  `setupTerragruntInputs`**, so `run.go` (platform-agnostic) calls it
  unconditionally. The old `SetTerragruntInputsAsEnvVars` env-population logic
  moves into the Windows build of `setupTerragruntInputs`; the exported
  `SetTerragruntInputsAsEnvVars` may be removed or kept as a thin Windows-only
  shim (decide in the plan; update/relocate `run_test.go` accordingly).
- `ToTerraformEnvVars`, `setTerragruntNullValuesRunCfg`, `NullTFVarsFile` are
  kept (used by the Windows path). Only **input-derived** env vars move; the
  `extra_arguments` env vars (`filterTerraformEnvVarsFromExtraArgsRunCfg`), IAM/
  creds env, and inherited user env are untouched.

### Modified: `internal/cli/commands/exec/exec.go`

- `runTargetCommand`: before running the child, set up the FIFO in the same
  `dir` the command runs in (`opts.WorkingDir` when `InDownloadDir`, else
  `opts.RootWorkingDir`) and `defer` its cleanup around the
  `RunActionWithHooks`/`shell.RunCommandWithOutput` call.

### Modified: `internal/prepare/prepare.go`

- `PrepareInputsAsEnvVars` and `PrepareInit` stop populating inputs themselves on
  **both** platforms (keep `CheckFolderContainsTerraformCode`). Inputs are set up
  at the seams instead: tofu commands via `runTerragruntWithConfig` (including the
  init that `PrepareInit` triggers), and the arbitrary `exec` child via
  `runTargetCommand`. The platform choice (FIFO vs env) is made there by
  `setupTerragruntInputs`.

## Edge cases

- **Commands that never read vars** (`init`, `version`, arbitrary `exec`): the
  poll loop spins harmlessly and is cancelled at cleanup; no deadlock.
- **Auto-init**: a recursive `runTerragruntWithConfig`, so init and the main
  command each get an independent FIFO and goroutine.
- **Parallel modules** (`run --all`): each module has a distinct working dir →
  distinct FIFO path; no collision. Init and main run sequentially within a dir.
- **Stale FIFO** from a crashed run: removed before `mkfifo`.
- **`EPIPE`** (reader opened then left before reading): logged at debug,
  non-fatal; loop continues / exits on cancel.
- **Context cancellation / signals**: cleanup cancels the writer and removes the
  FIFO; the `defer` runs on every return path.

## Testing

- **Unit** (`inputs_tfvars_test.go`, Unix): a goroutine reader (`os.ReadFile`)
  receives the exact JSON including `nil` values; an input whose `TF_VAR_x` is
  preset is omitted; `mkfifo` failure returns an error (fail-closed); cleanup
  removes the FIFO and the goroutine exits with no reader present.
- **Rewrite** `TestSetTerragruntInputsAsEnvVars` (`run_test.go`) on Unix to
  assert `opts.Env` is **not** populated with `TF_VAR_*` from inputs (and the
  Windows path still is, if exercised).
- **Integration** (`/test`, gated on a real tofu/terraform binary): many/large
  inputs that previously exceeded `ARG_MAX` now succeed; values are read by
  tofu; the env of the tofu process contains no `TF_VAR_<input>`.

## Out of scope

- The broader #4402 proposal of a `variables {}` config block with
  `kind`/`validate`/`nullable`. This spec is the focused mechanism swap only.
- Windows parity for the `ARG_MAX`/secrecy guarantees (explicitly deferred per
  D8).

## Risks

- **Precedence delta** (above) — mitigated by the dot-prefixed name and
  documented; the only user-visible semantic change.
- **`x/sys/unix` dependency** for `Mkfifo` — confirm it is already in `go.mod`
  (it almost certainly is, transitively); otherwise add it. Implementation
  detail for the plan.
