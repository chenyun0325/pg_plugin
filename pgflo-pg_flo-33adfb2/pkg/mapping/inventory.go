package mapping

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// Fixed-scale PG numeric values must not be rounded when writing legacy int4.
// Signed quantities are intentional: the current inventory contract permits
// negative counts, and NULL remains NULL.
func int32Exact(_ TransformContext, value interface{}, args []string) (interface{}, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("int32_exact takes no additional arguments")
	}
	if value == nil {
		return nil, nil
	}
	// pgoutput's numeric codec returns pgtype.Numeric, while YAML preview and
	// pgcat's JSON decoder provide a string. Value preserves the exact decimal
	// representation; fmt.Sprint on Numeric would stringify its Go struct.
	if number, ok := value.(pgtype.Numeric); ok {
		var err error
		value, err = number.Value()
		if err != nil || value == nil {
			return value, err
		}
	}
	parts := strings.Split(fmt.Sprint(value), ".")
	if len(parts) > 2 || (len(parts) == 2 && (parts[1] == "" || strings.Trim(parts[1], "0") != "")) {
		return nil, fmt.Errorf("int32_exact requires an integral fixed-point value")
	}
	number, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return nil, fmt.Errorf("int32_exact: %w", err)
	}
	return number, nil
}

func sourceTableKey(ctx TransformContext, value interface{}, args []string) (interface{}, error) {
	if value == nil || len(args) != 0 {
		return nil, fmt.Errorf("source_table_key needs a non-NULL value and no additional arguments")
	}
	parts := strings.SplitN(ctx.SourceTable, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("source_table_key needs schema.table")
	}
	return parts[1] + "_" + fmt.Sprint(value), nil
}

func warehouseType(ctx TransformContext, _ interface{}, args []string) (interface{}, error) {
	if len(args) != 0 {
		return nil, fmt.Errorf("warehouse_type takes no additional arguments")
	}
	parts := strings.SplitN(ctx.SourceTable, ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("warehouse_type needs schema.table")
	}
	switch {
	case strings.HasPrefix(parts[1], "sys_warehouse_outbound_"):
		return int64(1), nil
	case parts[1] == "mem_supplier_warehouse_outbound":
		return int64(2), nil
	case parts[1] == "thd_warehouse_outbound":
		return int64(3), nil
	default:
		return nil, fmt.Errorf("warehouse_type: unsupported table %s", ctx.SourceTable)
	}
}
