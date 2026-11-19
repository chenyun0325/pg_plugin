package cmd

import (
	"fmt"
	"strings"

	"github.com/pgflo/pg_flo/pkg/mapping"
)

// A compiled extension example, available from YAML in all job commands.
// Deploy identical binaries on capture/worker nodes and version the mapping.
func init() {
	if err := mapping.RegisterExtension("trim_space", func(_ mapping.TransformContext, value interface{}, args []string) (interface{}, error) {
		if len(args) != 0 {
			return nil, fmt.Errorf("trim_space takes no arguments")
		}
		if value == nil {
			return nil, nil
		}
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("trim_space requires a string")
		}
		return strings.TrimSpace(text), nil
	}); err != nil {
		panic(err)
	}
}
