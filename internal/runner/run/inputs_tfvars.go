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

// serveInputsFIFO polls the FIFO open for writing (non-blocking) until a reader
// connects, streams the payload once, and returns. Opening O_WRONLY on a FIFO with
// no reader returns ENXIO, so we poll until OpenTofu/Terraform opens the read end,
// re-checking ctx/stop each iteration so a command that never reads the file (e.g.
// init, version, or a non-tofu exec target) does not block cleanup.
//
// We serve exactly once: OpenTofu/Terraform reads each auto-tfvars file a single
// time per process (its variable collection is cached), and each child process gets
// its own FIFO. Re-opening after the write would risk delivering a duplicate copy to
// the same still-draining reader before it observes EOF (the writer becomes present
// again before the reader's next read returns end-of-file).
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

		// A reader has opened the FIFO. Stream the payload and close to deliver EOF.
		// EPIPE here means the reader went away early; it is non-fatal.
		if _, err := f.Write(data); err != nil {
			l.Debugf("writing to inputs FIFO %s: %v", path, err)
		}

		if err := f.Close(); err != nil {
			l.Debugf("closing inputs FIFO %s: %v", path, err)
		}

		return
	}
}
