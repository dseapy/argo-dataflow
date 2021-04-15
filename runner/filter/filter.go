package filter

import (
	"context"
	"fmt"

	"github.com/antonmedv/expr"
	"github.com/argoproj-labs/argo-dataflow/runner/util"
)

func Exec(ctx context.Context, x string) error {
	prog, err := expr.Compile(x)
	if err != nil {
		return fmt.Errorf("failed to compile %q: %w", x, err)
	}
	return util.Do(ctx, func(msg []byte) ([][]byte, error) {
		res, err := expr.Run(prog, util.ExprEnv(msg))
		if err != nil {
			return nil, fmt.Errorf("failed to run program %x: %w", x, err)
		}
		accept, ok := res.(bool)
		if !ok {
			return nil, fmt.Errorf("%q must return bool", x)
		}
		if accept {
			return [][]byte{msg}, nil
		} else {
			return nil, nil
		}
	})
}