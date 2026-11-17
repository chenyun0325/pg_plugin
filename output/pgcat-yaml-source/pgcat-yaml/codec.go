package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func decodeValue(b []byte, oid uint32) (any, error) {
	s := string(b)
	switch oid {
	case 16:
		return strconv.ParseBool(s)
	case 20, 21, 23, 26, 700, 701, 1700:
		if s == "NaN" || strings.Contains(s, "Infinity") {
			return nil, fmt.Errorf("nonfinite numeric unsupported")
		}
		return json.Number(s), nil
	case 114, 3802:
		var v any
		d := json.NewDecoder(strings.NewReader(s))
		d.UseNumber()
		e := d.Decode(&v)
		return v, e
	default:
		return s, nil
	}
}
