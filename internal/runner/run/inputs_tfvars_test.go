//go:build !windows

package run_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gruntwork-io/terragrunt/internal/runner/run"
	"github.com/gruntwork-io/terragrunt/test/helpers/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetupTerragruntInputsServesFIFO(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	env := map[string]string{"TF_VAR_skipme": "preset"}

	inputs := map[string]any{
		"foo":    "bar",
		"n":      nil,
		"esc":    "x ${y} z",
		"skipme": "ignored",
	}

	cleanup, err := run.SetupTerragruntInputs(context.Background(), logger.CreateLogger(), dir, inputs, env)
	require.NoError(t, err)

	defer cleanup()

	// On Unix the env map must not be mutated.
	assert.Equal(t, map[string]string{"TF_VAR_skipme": "preset"}, env)

	data := readFIFOWithTimeout(t, filepath.Join(dir, run.InputsTFVarsFile), 5*time.Second)

	var got map[string]any

	require.NoError(t, json.Unmarshal(data, &got))

	// null is preserved; ${...} is escaped to $${...}; and the input already set as a
	// TF_VAR_ env var is omitted so the user's environment value keeps precedence.
	assert.Equal(t, map[string]any{"foo": "bar", "n": nil, "esc": "x $${y} z"}, got)
}

func TestSetupTerragruntInputsNoInputs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	cleanup, err := run.SetupTerragruntInputs(context.Background(), logger.CreateLogger(), dir, nil, nil)
	require.NoError(t, err)

	defer cleanup()

	_, statErr := os.Stat(filepath.Join(dir, run.InputsTFVarsFile))
	assert.True(t, os.IsNotExist(statErr), "no FIFO should be created when there are no inputs")
}

func TestSetupTerragruntInputsAllSkipped(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	env := map[string]string{"TF_VAR_only": "preset"}

	cleanup, err := run.SetupTerragruntInputs(context.Background(), logger.CreateLogger(), dir, map[string]any{"only": "x"}, env)
	require.NoError(t, err)

	defer cleanup()

	_, statErr := os.Stat(filepath.Join(dir, run.InputsTFVarsFile))
	assert.True(t, os.IsNotExist(statErr), "no FIFO when every input is already set as a TF_VAR_ env var")
}

func TestSetupTerragruntInputsCleanupWithoutReader(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	cleanup, err := run.SetupTerragruntInputs(context.Background(), logger.CreateLogger(), dir, map[string]any{"foo": "bar"}, nil)
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

	_, statErr := os.Stat(filepath.Join(dir, run.InputsTFVarsFile))
	assert.True(t, os.IsNotExist(statErr))
}

func readFIFOWithTimeout(t *testing.T, path string, timeout time.Duration) []byte {
	t.Helper()

	type result struct {
		err  error
		data []byte
	}

	ch := make(chan result, 1)

	go func() {
		data, err := os.ReadFile(path)
		ch <- result{err: err, data: data}
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
