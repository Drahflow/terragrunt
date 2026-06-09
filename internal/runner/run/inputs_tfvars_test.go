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
