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
