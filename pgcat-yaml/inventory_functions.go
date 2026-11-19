package main

import (
	"fmt"
	"strconv"
	"strings"
)

// PostgreSQL numeric(24,6) travels as fixed-scale text (e.g. "20.000000").
// Legacy int4 targets require an integer spelling; never round fractional data.
func init() { Register("int32_exact", int32Exact) }

func int32Exact(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("int32_exact needs one arg")
	}
	if args[0] == nil {
		return nil, nil
	}
	parts := strings.Split(fmt.Sprint(args[0]), ".")
	if len(parts) > 2 || (len(parts) == 2 && (parts[1] == "" || strings.Trim(parts[1], "0") != "")) {
		return nil, fmt.Errorf("int32_exact requires an integral fixed-point value")
	}
	n, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("int32_exact: %w", err)
	}
	return n, nil
}
