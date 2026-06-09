# Terragrunt inputs via FIFO `*.auto.tfvars.json` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop passing Terragrunt `inputs` to OpenTofu/Terraform as `TF_VAR_*` environment variables; on Unix stream them through a named pipe (FIFO) auto-loaded as `.terragrunt-inputs.auto.tfvars.json`, keeping input values out of both the environment (fixes ARG_MAX/E2BIG, [#4402](https://github.com/gruntwork-io/terragrunt/issues/4402)) and the filesystem.

**Architecture:** A build-tagged helper `run.SetupTerragruntInputs` is invoked at each child-process exec seam. On Unix it creates a `0600` FIFO in the working dir plus a goroutine that streams the JSON-encoded inputs to each reader, returning a cleanup func that stops the goroutine and removes the FIFO. On Windows (no POSIX FIFOs) it retains the legacy `TF_VAR_*` + null-vars-file behavior. Inputs already present as a user `TF_VAR_<name>` are omitted so the user's env value keeps precedence. Interpolation (`${...}`) in string values is escaped so OpenTofu/Terraform treats them literally.

**Tech Stack:** Go 1.26, `golang.org/x/sys/unix` (Mkfifo — already a direct dep), `encoding/json`, OpenTofu/Terraform auto-tfvars loading.

**Spec:** `docs/superpowers/specs/2026-06-09-terragrunt-inputs-fifo-tfvars-design.md`

---

## Pre-flight (read once)

- Branch `inputs-fifo-tfvars` is already checked out; `origin` is **upstream** `gruntwork-io/terragrunt` — **never push**.
- `go version` → go1.26.0 is available. Unit tests (Tasks 1–4) run locally.
- No `tofu`/`terraform` binary is on PATH here, so the end-to-end tests (Task 5) only run in CI. Build them, don't expect to run them locally.
- Lint uses `make run-lint` (golangci-lint via mise; `staticcheck`, `wsl_v5`, `govet fieldalignment`). Test files are exempt from `wsl`/`mnd`/`errcheck`/`unparam`. After each non-test code change run `gofmt -w <files>` and, if available, `make run-lint-fix`.
- `depguard` only forbids `hashicorp/go-getter`; `golang.org/x/sys/unix` is allowed.

## File Structure

- **Modify** `internal/util/jsons.go` → add exported `EscapeTerraformInterpolation` (Task 1).
- **Create** `internal/runner/run/inputs_tfvars.go` (`//go:build !windows`) → FIFO mechanism: `SetupTerragruntInputs`, `buildInputsTFVarsJSON`, `serveInputsFIFO`, consts (Task 2).
- **Create** `internal/runner/run/inputs_tfvars_test.go` (`//go:build !windows`, `package run`) → unit tests (Task 2).
- **Create** `internal/runner/run/inputs_tfvars_windows.go` (`//go:build windows`) → legacy env-var fallback (Task 3).
- **Modify (one atomic task)** `internal/runner/run/run.go`, `internal/runner/run/run_test.go`, `internal/cli/commands/exec/exec.go`, `internal/prepare/prepare.go` → switch all seams to `SetupTerragruntInputs` and delete the old env path (Task 4). These are coupled: deleting `SetTerragruntInputsAsEnvVars` breaks its callers unless they change together.
- **Modify** `test/fixtures/inputs-interpolation/*` and `test/integration_test.go` (Task 5).
- **Modify** docs + changelog (Task 6).

---

### Task 1: `util.EscapeTerraformInterpolation`

**Files:**
- Modify: `internal/util/jsons.go`
- Test: `internal/util/jsons_test.go`

- [ ] **Step 1: Write the failing test**

Append to `internal/util/jsons_test.go` (package `util`; create with `package util` + the imports below if it doesn't exist):

```go
func TestEscapeTerraformInterpolation(t *testing.T) {
	t.Parallel()

	// Top-level scalar string IS escaped (unlike AsTerraformEnvVarJSONValue, which
	// leaves scalar env-var strings raw). This is required for the .tfvars.json path.
	got, err := EscapeTerraformInterpolation("literal ${x} end")
	require.NoError(t, err)
	assert.Equal(t, "literal $${x} end", got)

	// Nested strings in maps are escaped; nil is preserved.
	got, err = EscapeTerraformInterpolation(map[string]any{"foo": "a ${b} c", "n": nil})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "a $${b} c", "n": nil}, got)

	// Non-string scalars pass through unchanged.
	got, err = EscapeTerraformInterpolation(42)
	require.NoError(t, err)
	assert.Equal(t, 42, got)
}
```

Ensure the file imports `"testing"`, `"github.com/stretchr/testify/assert"`, `"github.com/stretchr/testify/require"`.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/util/ -run TestEscapeTerraformInterpolation -v`
Expected: FAIL — `undefined: EscapeTerraformInterpolation`.

- [ ] **Step 3: Write minimal implementation**

Append to `internal/util/jsons.go`:

```go
// EscapeTerraformInterpolation escapes HCL interpolation patterns (${...}) in every
// string within the value tree, including a top-level string scalar. Use this for
// values written to a .tfvars.json file, where OpenTofu/Terraform's HCL-JSON parser
// treats all string values as templates. (AsTerraformEnvVarJSONValue deliberately
// does NOT escape scalar strings, because scalar TF_VAR_* env vars are taken
// literally.) Nil maps/slices are preserved as nil.
func EscapeTerraformInterpolation(value any) (any, error) {
	return escapeInterpolationPatternsInValue(value, 0)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/util/ -run TestEscapeTerraformInterpolation -v`
Expected: PASS.

- [ ] **Step 5: gofmt + commit**

```bash
gofmt -w internal/util/jsons.go internal/util/jsons_test.go
git add internal/util/jsons.go internal/util/jsons_test.go
git commit -m "$(printf 'feat(util): add EscapeTerraformInterpolation for tfvars-file values\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

---

### Task 2: Unix FIFO helper

**Files:**
- Create: `internal/runner/run/inputs_tfvars.go`
- Test: `internal/runner/run/inputs_tfvars_test.go`

- [ ] **Step 1: Write the failing tests**

Create `internal/runner/run/inputs_tfvars_test.go`:

```go
//go:build !windows

package run

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gruntwork-io/terragrunt/test/helpers/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildInputsTFVarsJSON(t *testing.T) {
	t.Parallel()

	// nil-valued inputs are included (as JSON null); ${...} is escaped; an input
	// whose TF_VAR_ is already set in env is omitted so the env value keeps winning.
	data, has, err := buildInputsTFVarsJSON(
		map[string]any{"foo": "a ${b} c", "n": nil, "skipme": "x"},
		map[string]string{"TF_VAR_skipme": "from-env"},
	)
	require.NoError(t, err)
	require.True(t, has)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, map[string]any{"foo": "a $${b} c", "n": nil}, got)

	// Nothing to stream once everything is skipped or empty.
	_, has, err = buildInputsTFVarsJSON(map[string]any{}, nil)
	require.NoError(t, err)
	assert.False(t, has)
}

func TestSetupTerragruntInputsServesFIFO(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	env := map[string]string{}

	cleanup, err := SetupTerragruntInputs(context.Background(), logger.CreateLogger(), dir, map[string]any{"foo": "bar"}, env)
	require.NoError(t, err)
	defer cleanup()

	// The env map must NOT be mutated on Unix.
	assert.Empty(t, env)

	data := readFIFOWithTimeout(t, filepath.Join(dir, InputsTFVarsFile), 5*time.Second)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, map[string]any{"foo": "bar"}, got)

	cleanup()
	_, statErr := os.Stat(filepath.Join(dir, InputsTFVarsFile))
	assert.True(t, os.IsNotExist(statErr), "FIFO should be removed after cleanup")
}

func TestSetupTerragruntInputsNoInputs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	cleanup, err := SetupTerragruntInputs(context.Background(), logger.CreateLogger(), dir, nil, nil)
	require.NoError(t, err)
	defer cleanup()

	_, statErr := os.Stat(filepath.Join(dir, InputsTFVarsFile))
	assert.True(t, os.IsNotExist(statErr), "no FIFO should be created when there are no inputs")
}

func TestSetupTerragruntInputsCleanupWithoutReader(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	cleanup, err := SetupTerragruntInputs(context.Background(), logger.CreateLogger(), dir, map[string]any{"foo": "bar"}, nil)
	require.NoError(t, err)

	// No reader ever opens the FIFO; cleanup must return promptly, not hang.
	done := make(chan struct{})
	go func() {
		cleanup()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup hung when no reader connected")
	}

	_, statErr := os.Stat(filepath.Join(dir, InputsTFVarsFile))
	assert.True(t, os.IsNotExist(statErr))
}

func readFIFOWithTimeout(t *testing.T, path string, timeout time.Duration) []byte {
	t.Helper()

	type result struct {
		data []byte
		err  error
	}

	ch := make(chan result, 1)
	go func() {
		data, err := os.ReadFile(path)
		ch <- result{data: data, err: err}
	}()

	select {
	case r := <-ch:
		require.NoError(t, r.err)
		return r.data
	case <-time.After(timeout):
		t.Fatalf("timed out reading FIFO %s", path)
		return nil
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/runner/run/ -run 'TestBuildInputsTFVarsJSON|TestSetupTerragruntInputs' -v`
Expected: FAIL to compile — `undefined: buildInputsTFVarsJSON`, `undefined: SetupTerragruntInputs`, `undefined: InputsTFVarsFile`.

- [ ] **Step 3: Write the implementation**

Create `internal/runner/run/inputs_tfvars.go`:

```go
//go:build !windows

package run

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/gruntwork-io/terragrunt/internal/tf"
	"github.com/gruntwork-io/terragrunt/internal/util"
	"github.com/gruntwork-io/terragrunt/pkg/log"
)

// InputsTFVarsFile is the named pipe (FIFO) Terragrunt creates in the working
// directory to stream input values to OpenTofu/Terraform as an auto-loaded tfvars
// file. The leading dot makes it sort before user-provided *.auto.tfvars files (so
// those still override Terragrunt inputs) and keeps it namespaced.
const InputsTFVarsFile = ".terragrunt-inputs.auto.tfvars.json"

// inputsFIFOMode is the FIFO permission mode: owner read/write only.
const inputsFIFOMode uint32 = 0o600

// fifoPollInterval is how often the writer re-checks for a reader (and for
// cancellation) while no reader has opened the FIFO.
const fifoPollInterval = 20 * time.Millisecond

// SetupTerragruntInputs makes Terragrunt inputs available to the child process
// without placing their values in the environment (avoiding ARG_MAX/E2BIG) or on
// disk. On Unix it creates a FIFO named InputsTFVarsFile in dir and streams the
// inputs, JSON-encoded, to whoever opens it (OpenTofu/Terraform auto-loads
// *.auto.tfvars.json). The returned cleanup stops the writer and removes the FIFO;
// callers MUST defer it. An input whose TF_VAR_<name> already exists in env is
// omitted so a user-set environment variable keeps precedence.
func SetupTerragruntInputs(ctx context.Context, l log.Logger, dir string, inputs map[string]any, env map[string]string) (func(), error) {
	noop := func() {}

	data, hasInputs, err := buildInputsTFVarsJSON(inputs, env)
	if err != nil {
		return noop, err
	}

	if !hasInputs {
		return noop, nil
	}

	path := filepath.Join(dir, InputsTFVarsFile)

	// Remove any stale node (e.g. a FIFO left by a crashed run) before recreating.
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return noop, fmt.Errorf("removing stale inputs file %s: %w", path, err)
	}

	if err := unix.Mkfifo(path, inputsFIFOMode); err != nil {
		return noop, fmt.Errorf("creating inputs FIFO %s: %w", path, err)
	}

	stop := make(chan struct{})

	var (
		once sync.Once
		wg   sync.WaitGroup
	)

	wg.Add(1)

	go func() {
		defer wg.Done()
		serveInputsFIFO(ctx, stop, l, path, data)
	}()

	cleanup := func() {
		once.Do(func() { close(stop) })
		wg.Wait()

		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			l.Debugf("removing inputs FIFO %s: %v", path, err)
		}
	}

	return cleanup, nil
}

// buildInputsTFVarsJSON encodes the inputs to stream as one JSON object
// (name -> value), preserving nulls. Inputs whose TF_VAR_<name> is already in env
// are skipped. String values have ${...} escaped so OpenTofu/Terraform treats them
// literally, matching the previous env-var behavior. The bool is false when there
// is nothing to stream.
func buildInputsTFVarsJSON(inputs map[string]any, env map[string]string) ([]byte, bool, error) {
	vars := make(map[string]any, len(inputs))

	for name, value := range inputs {
		if _, ok := env[fmt.Sprintf(tf.EnvNameTFVarFmt, name)]; ok {
			continue
		}

		escaped, err := util.EscapeTerraformInterpolation(value)
		if err != nil {
			return nil, false, fmt.Errorf("escaping input %q: %w", name, err)
		}

		vars[name] = escaped
	}

	if len(vars) == 0 {
		return nil, false, nil
	}

	data, err := json.Marshal(vars)
	if err != nil {
		return nil, false, fmt.Errorf("encoding inputs as JSON: %w", err)
	}

	return data, true, nil
}

// serveInputsFIFO repeatedly opens the FIFO for writing (non-blocking) and streams
// data to each reader that connects, until ctx is cancelled or stop is closed.
// Opening O_WRONLY on a FIFO with no reader returns ENXIO, so we poll until
// OpenTofu/Terraform opens the read end. Writing after a reader has gone away
// yields EPIPE, which is non-fatal.
func serveInputsFIFO(ctx context.Context, stop <-chan struct{}, l log.Logger, path string, data []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		default:
		}

		f, err := os.OpenFile(path, os.O_WRONLY|unix.O_NONBLOCK, 0)
		if err != nil {
			if !errors.Is(err, unix.ENXIO) {
				l.Debugf("opening inputs FIFO %s for writing: %v", path, err)
			}

			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-time.After(fifoPollInterval):
			}

			continue
		}

		if _, err := f.Write(data); err != nil {
			l.Debugf("writing to inputs FIFO %s: %v", path, err)
		}

		if err := f.Close(); err != nil {
			l.Debugf("closing inputs FIFO %s: %v", path, err)
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/runner/run/ -run 'TestBuildInputsTFVarsJSON|TestSetupTerragruntInputs' -v`
Expected: PASS (all four tests).

- [ ] **Step 5: gofmt + commit**

```bash
gofmt -w internal/runner/run/inputs_tfvars.go internal/runner/run/inputs_tfvars_test.go
git add internal/runner/run/inputs_tfvars.go internal/runner/run/inputs_tfvars_test.go
git commit -m "$(printf 'feat(run): stream inputs through a FIFO .auto.tfvars.json (unix)\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

---

### Task 3: Windows fallback (legacy env vars)

**Files:**
- Create: `internal/runner/run/inputs_tfvars_windows.go`

> No unit test (can't run Windows binaries locally); verified by cross-compilation. It reuses the still-present `ToTerraformEnvVars` and `NullTFVarsFile` from `run.go` (those are removed from the call path only in Task 4, but the symbols remain).

- [ ] **Step 1: Write the implementation**

Create `internal/runner/run/inputs_tfvars_windows.go`:

```go
//go:build windows

package run

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/gruntwork-io/terragrunt/pkg/log"
)

// SetupTerragruntInputs on Windows retains the legacy mechanism: POSIX FIFOs are
// unavailable, so input values are placed in TF_VAR_* environment variables (skipping
// any the caller already set) and null-valued inputs are written to NullTFVarsFile in
// dir. The ARG_MAX/E2BIG fix and the off-environment/off-disk guarantees do NOT apply
// on Windows; this is a documented platform limitation. ctx is unused here.
func SetupTerragruntInputs(_ context.Context, l log.Logger, dir string, inputs map[string]any, env map[string]string) (func(), error) {
	noop := func() {}

	asEnvVars, err := ToTerraformEnvVars(l, inputs)
	if err != nil {
		return noop, err
	}

	for key, value := range asEnvVars {
		if _, ok := env[key]; !ok {
			env[key] = value
		}
	}

	nullVarsFile, err := writeNullVarsFile(dir, inputs)
	if err != nil {
		return noop, err
	}

	if nullVarsFile == "" {
		return noop, nil
	}

	cleanup := func() {
		if rmErr := os.Remove(nullVarsFile); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			l.Debugf("removing null values file %s: %v", nullVarsFile, rmErr)
		}
	}

	return cleanup, nil
}

// writeNullVarsFile writes null-valued inputs to NullTFVarsFile in dir so
// OpenTofu/Terraform auto-loads them (it cannot accept null via TF_VAR_*). Returns
// "" when there are no null inputs.
func writeNullVarsFile(dir string, inputs map[string]any) (string, error) {
	jsonEmptyVars := make(map[string]any)

	for varName, varValue := range inputs {
		if varValue == nil {
			jsonEmptyVars[varName] = nil
		}
	}

	if len(jsonEmptyVars) == 0 {
		return "", nil
	}

	jsonContents, err := json.MarshalIndent(jsonEmptyVars, "", "  ")
	if err != nil {
		return "", err
	}

	varFile := filepath.Join(dir, NullTFVarsFile)

	const ownerReadWritePermissions = 0o600

	if err := os.WriteFile(varFile, jsonContents, os.FileMode(ownerReadWritePermissions)); err != nil {
		return "", err
	}

	return varFile, nil
}
```

- [ ] **Step 2: Verify it cross-compiles for Windows**

Run: `GOOS=windows go build ./internal/runner/run/`
Expected: builds with no error. (`SetupTerragruntInputs` is defined once on Windows by this file; `ToTerraformEnvVars`/`NullTFVarsFile` still exist in `run.go`. It is exported and currently uncalled on Windows — that's fine for a build.)

- [ ] **Step 3: gofmt + commit**

```bash
gofmt -w internal/runner/run/inputs_tfvars_windows.go
git add internal/runner/run/inputs_tfvars_windows.go
git commit -m "$(printf 'feat(run): retain TF_VAR_* inputs fallback on windows\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

---

### Task 4: Switch all seams to `SetupTerragruntInputs` and remove the env path (atomic)

**Files (all edited before building — one commit):**
- Modify: `internal/runner/run/run.go`
- Modify: `internal/runner/run/run_test.go`
- Modify: `internal/cli/commands/exec/exec.go`
- Modify: `internal/prepare/prepare.go`

> Why atomic: deleting `SetTerragruntInputsAsEnvVars` breaks `prepare.go` and `run.go`, and renaming `PrepareInputsAsEnvVars` breaks `exec.go`. Make all edits, then build once.

- [ ] **Step 1: `run.go` — replace the input/null-vars call site**

In `runTerragruntWithConfig`, delete the old env-var call (currently ~250–253):

```go
	if err := SetTerragruntInputsAsEnvVars(l, opts, cfg); err != nil {
		return err
	}

```

Then replace the null-vars block (currently ~264–276):

```go
	// Write null-valued inputs to a tfvars.json file that OpenTofu/Terraform will auto-load.
	nullVarsFile, err := setTerragruntNullValuesRunCfg(opts, cfg)
	if err != nil {
		return err
	}

	defer func() {
		if nullVarsFile != "" {
			if removeErr := os.Remove(nullVarsFile); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				l.Debugf("Failed to remove null values file %s: %v", nullVarsFile, removeErr)
			}
		}
	}()
```

with:

```go
	// Make Terragrunt inputs available to OpenTofu/Terraform without putting their
	// values in the environment (avoids ARG_MAX/E2BIG) or on disk. On Unix this
	// streams them through a FIFO auto-loaded as *.auto.tfvars.json; on Windows it
	// falls back to TF_VAR_* env vars. See inputs_tfvars.go / inputs_tfvars_windows.go.
	cleanupInputs, err := SetupTerragruntInputs(ctx, l, opts.WorkingDir, cfg.Inputs, opts.Env)
	if err != nil {
		return err
	}

	defer cleanupInputs()
```

Placement is unchanged (after the init/non-init branching, before `checkProtectedModuleRunCfg`). This ordering matters: the recursive auto-init call sets up and removes its own FIFO before this one is created, so there is never a same-path collision.

- [ ] **Step 2: `run.go` — delete the two now-orphaned functions**

Delete `SetTerragruntInputsAsEnvVars` entirely (function + doc comment, ~395–415) and `setTerragruntNullValuesRunCfg` entirely (function + doc comment, ~763–794). Keep `ToTerraformEnvVars` (used by the Windows file and by `TestToTerraformEnvVars`) and the `NullTFVarsFile` const (used by the Windows file).

- [ ] **Step 3: `run.go` — drop now-unused imports**

In the import block, delete the line `"encoding/json"` and the line `"errors"`. They were used only by the code removed in Steps 1–2 (`json.MarshalIndent` at old line 780; `errors.Is` at old line 272). `os`, `filepath`, `fmt`, `sync` stay (used elsewhere).

- [ ] **Step 4: `run_test.go` — delete the obsolete test**

Delete the entire `TestSetTerragruntInputsAsEnvVars` function (lines 21–87). Leave every other test (including `TestToTerraformEnvVars`); no imports become unused (`configbridge`/`iacargs`/`util`/`helpers`/`options`/`runcfg`/`logger` are all used by other tests in the file).

- [ ] **Step 5: `exec.go` — create the FIFO around the exec child + fix the caller name**

In `runTargetCommand`, after `dir` is finalized and before `runOpts := configbridge.NewRunOptions(opts)`, insert:

```go
	cleanupInputs, err := run.SetupTerragruntInputs(ctx, l, dir, cfg.Inputs, opts.Env)
	if err != nil {
		return err
	}

	defer cleanupInputs()

```

(`run` is already imported. On Unix this drops a FIFO in `dir`; on Windows it sets env on `opts.Env`, the same map `ShellRunOptsFromOpts(opts)` hands to the child.)

Then at line ~55 change `prepare.PrepareInputsAsEnvVars(l, updatedOpts, runCfg)` to `prepare.PrepareInputs(l, updatedOpts, runCfg)`.

- [ ] **Step 6: `prepare.go` — rename + drop env calls**

Replace `PrepareInputsAsEnvVars` (~174–185) with:

```go
// PrepareInputs verifies the working directory contains OpenTofu/Terraform code.
// Inputs are no longer set as environment variables here; they are streamed to the
// child process at run time (see run.SetupTerragruntInputs). It requires
// PrepareGenerate to have been called first.
func PrepareInputs(_ log.Logger, opts *options.TerragruntOptions, _ *runcfg.RunConfig) error {
	return run.CheckFolderContainsTerraformCode(configbridge.NewRunOptions(opts))
}
```

In `PrepareInit` (~189–209) remove:

```go
	if err := run.SetTerragruntInputsAsEnvVars(l, runOpts, cfg); err != nil {
		return err
	}

```

Update `PrepareInit`'s doc comment "It requires PrepareInputsAsEnvVars to have been called first." → "It requires PrepareInputs to have been called first." Update the pipeline comment near the top of the file (~10–11): "4. PrepareInputsAsEnvVars - Sets inputs as environment variables" → "4. PrepareInputs - Verifies OpenTofu/Terraform code is present (inputs are streamed at run time)".

- [ ] **Step 7: Build (both platforms), vet, test**

Run:
```bash
gofmt -w internal/runner/run/run.go internal/runner/run/run_test.go internal/cli/commands/exec/exec.go internal/prepare/prepare.go
go build ./... && GOOS=windows go build ./... && go vet ./... && go test ./internal/runner/run/ ./internal/prepare/ ./internal/cli/commands/exec/ -count=1
```
Expected: both builds succeed (this is the authoritative cross-compile check — `SetupTerragruntInputs` resolves to the unix impl on linux and the windows impl on windows); vet clean; tests PASS (FIFO tests pass; `TestSetTerragruntInputsAsEnvVars` is gone). If `make run-lint-fix` is available, run it and fix any `wsl_v5`/`fieldalignment` findings in the new files.

- [ ] **Step 8: Commit**

```bash
git add internal/runner/run/run.go internal/runner/run/run_test.go internal/cli/commands/exec/exec.go internal/prepare/prepare.go
git commit -m "$(printf 'feat(run,exec,prepare): pass inputs via FIFO tfvars, remove TF_VAR_* env path\n\nFixes ARG_MAX/E2BIG with large/many inputs (gruntwork-io/terragrunt#4402)\nand keeps input values out of the environment and off disk on Unix.\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

---

### Task 5: End-to-end regression + new coverage (CI-run; needs a tofu/terraform binary)

**Files:**
- Modify: `test/fixtures/inputs-interpolation/main.tf`, `test/fixtures/inputs-interpolation/terragrunt.hcl`
- Modify: `test/integration_test.go` (`TestInputsWithInterpolationPatterns` ~1468; add `TestInputsLargeValueThroughFIFO`)

> The existing `TestInputsPassedThroughCorrectly` and `TestRunCommand` (fixture `fixtures/inputs`) are the primary regression net — they already assert inputs reach OpenTofu/Terraform end-to-end and must keep passing unchanged. This task adds a scalar-string interpolation assertion and a large-value test.

- [ ] **Step 1: Extend the interpolation fixture with a scalar string**

Append to `test/fixtures/inputs-interpolation/main.tf`:

```hcl
variable "string_with_interpolation" {
  type = string
}

output "string_with_interpolation" {
  value = var.string_with_interpolation
}
```

Set `test/fixtures/inputs-interpolation/terragrunt.hcl` to:

```hcl
inputs = {
  map_with_interpolation    = jsondecode(file("stuff.json"))
  string_with_interpolation = "literal $${not_a_var} end"
}
```

(`$${...}` in HCL is the literal `${...}`, so the input value is the literal string `literal ${not_a_var} end` — exactly what should round-trip. This exercises the new scalar-escaping path.)

- [ ] **Step 2: Assert the scalar round-trips literally**

In `TestInputsWithInterpolationPatterns`, after the existing `map_with_interpolation` assertions, add:

```go
	strOutput, ok := outputs["string_with_interpolation"]
	require.True(t, ok, "string_with_interpolation output not found")
	assert.Equal(t, "literal ${not_a_var} end", strOutput.Value)
```

- [ ] **Step 3: Add a large-value test (the ARG_MAX scenario)**

Add to `test/integration_test.go`:

```go
// TestInputsLargeValueThroughFIFO exercises a large input value that, passed as a
// TF_VAR_* environment variable, could contribute to ARG_MAX/E2BIG (#4402). With the
// FIFO-backed *.auto.tfvars.json path it round-trips regardless of size.
func TestInputsLargeValueThroughFIFO(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	large := strings.Repeat("x", 512*1024)

	require.NoError(t, os.WriteFile(filepath.Join(tmp, "main.tf"), []byte(
		"variable \"big\" { type = string }\noutput \"big_len\" { value = length(var.big) }\n",
	), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(tmp, "terragrunt.hcl"), []byte(
		"inputs = {\n  big = \""+large+"\"\n}\n",
	), 0o644))

	helpers.RunTerragrunt(t, "terragrunt apply -auto-approve --non-interactive --working-dir "+tmp)

	stdout := bytes.Buffer{}
	stderr := bytes.Buffer{}
	require.NoError(t, helpers.RunTerragruntCommand(t, "terragrunt output -no-color -json --non-interactive --working-dir "+tmp, &stdout, &stderr))

	outputs := map[string]helpers.TerraformOutput{}
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &outputs))
	assert.EqualValues(t, len(large), outputs["big_len"].Value)
}
```

Ensure `test/integration_test.go` imports `"os"`, `"strings"`, `"path/filepath"`, `"bytes"` (most are already present; `goimports` resolves any missing one).

- [ ] **Step 4: Verify (CI / where a binary exists)**

Run (only meaningful with `tofu`/`terraform` installed): `go test ./test/ -run 'TestInputsWithInterpolationPatterns|TestInputsPassedThroughCorrectly|TestInputsLargeValueThroughFIFO' -v -count=1`
Expected: PASS. Locally without a binary, instead confirm it compiles: `go vet ./test/`.

- [ ] **Step 5: gofmt + commit**

```bash
gofmt -w test/integration_test.go
git add test/integration_test.go test/fixtures/inputs-interpolation/
git commit -m "$(printf 'test: cover scalar interpolation and large inputs through the inputs FIFO\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

---

### Task 6: Docs and changelog

**Files:**
- Modify: the inputs documentation page under `docs/` (located in Step 1)
- Modify: changelog (located in Step 1)

- [ ] **Step 1: Locate the docs to update**

Run: `grep -rln "TF_VAR_\|auto.tfvars\|inputs" docs/ | head` and `ls CHANGELOG.md docs/ 2>/dev/null`
Expected: identifies the page describing how `inputs` are passed to OpenTofu/Terraform and the changelog location.

- [ ] **Step 2: Document the new behavior**

In the inputs documentation page, add a short section stating:
- On Unix, inputs are passed via a `0600` named pipe `.terragrunt-inputs.auto.tfvars.json` in the working directory (values never touch the environment or disk), fixing ARG_MAX/E2BIG with large/many inputs.
- **Precedence change:** inputs now load as an auto-tfvars file, higher precedence than a module's `terraform.tfvars` (previously env-var precedence let `terraform.tfvars` win). A user's own `*.auto.tfvars` still overrides Terragrunt inputs (the Terragrunt file is dot-prefixed, sorts first), and a user-set `TF_VAR_<name>` is left untouched.
- **Windows:** retains `TF_VAR_*` environment variables (FIFOs unavailable); the ARG_MAX/secrecy benefits do not apply there.
- **`exec`:** `terragrunt exec -- <prog>` no longer exposes inputs via the environment; only a `<prog>` that reads `*.auto.tfvars.json` (i.e. tofu/terraform) will see them.

- [ ] **Step 3: Add a changelog entry** mirroring the summary above (behavior change + #4402 fix).

- [ ] **Step 4: Commit**

```bash
git add docs/ CHANGELOG.md 2>/dev/null || git add docs/
git commit -m "$(printf 'docs: describe FIFO-based inputs passing and precedence change\n\nCo-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>')"
```

---

## Final verification

- [ ] `gofmt -l internal/ test/` prints nothing.
- [ ] `go build ./... && GOOS=windows go build ./...` both succeed.
- [ ] `go vet ./...` is clean.
- [ ] `go test ./internal/... -count=1` passes.
- [ ] If available: `make run-lint` passes (especially `wsl_v5`, `staticcheck`, `govet fieldalignment` on the new files).
- [ ] `git log --oneline` shows one commit per task on `inputs-fifo-tfvars`; nothing pushed.
