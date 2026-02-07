// Package filter provides utilities for processing JMAP FilterOperator trees.
package filter

import (
	"fmt"
	"reflect"

	"github.com/jarrod-lowe/jmap-service-libs/jmaperror"
	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
)

// IsFilterOperator returns true if the args contain an "operator" key,
// indicating this is a FilterOperator rather than a flat FilterCondition.
func IsFilterOperator(args plugincontract.Args) bool {
	return args.Has("operator")
}

// FlattenFilter recursively flattens a FilterOperator tree into a single
// flat FilterCondition map. It returns an unsupportedFilter error for
// operators that cannot be represented as a flat condition (OR with multiple
// conditions, NOT).
func FlattenFilter(filterArg plugincontract.Args) (plugincontract.Args, error) {
	if !IsFilterOperator(filterArg) {
		return filterArg, nil
	}

	op, ok := filterArg.String("operator")
	if !ok {
		return nil, jmaperror.UnsupportedFilter("filter operator must be a string")
	}

	conditionsRaw, exists := filterArg["conditions"]
	if !exists {
		return nil, jmaperror.UnsupportedFilter("FilterOperator missing conditions")
	}
	conditions, ok := conditionsRaw.([]any)
	if !ok {
		return nil, jmaperror.UnsupportedFilter("conditions must be an array")
	}

	switch op {
	case "AND":
		return flattenAND(conditions)
	case "OR":
		if len(conditions) == 1 {
			cond, ok := conditions[0].(map[string]any)
			if !ok {
				return nil, jmaperror.UnsupportedFilter("condition must be an object")
			}
			return FlattenFilter(plugincontract.Args(cond))
		}
		return nil, jmaperror.UnsupportedFilter("OR filter with multiple conditions is not supported")
	case "NOT":
		return nil, jmaperror.UnsupportedFilter("NOT filter is not supported")
	default:
		return nil, jmaperror.UnsupportedFilter(fmt.Sprintf("unknown filter operator: %s", op))
	}
}

// flattenAND merges all conditions in an AND operator into a single flat map.
func flattenAND(conditions []any) (plugincontract.Args, error) {
	merged := plugincontract.Args{}

	for _, condRaw := range conditions {
		cond, ok := condRaw.(map[string]any)
		if !ok {
			return nil, jmaperror.UnsupportedFilter("condition must be an object")
		}

		flattened, err := FlattenFilter(plugincontract.Args(cond))
		if err != nil {
			return nil, err
		}

		if err := mergeInto(merged, flattened); err != nil {
			return nil, err
		}
	}

	return merged, nil
}

// mergeInto merges src keys into dst. For array values ([]any), it concatenates.
// For scalar values, it requires equality or returns an unsupportedFilter error.
func mergeInto(dst, src plugincontract.Args) error {
	for key, srcVal := range src {
		existing, exists := dst[key]
		if !exists {
			dst[key] = srcVal
			continue
		}

		// Both values are slices — concatenate
		existingSlice, existingIsSlice := existing.([]any)
		srcSlice, srcIsSlice := srcVal.([]any)
		if existingIsSlice && srcIsSlice {
			dst[key] = append(existingSlice, srcSlice...)
			continue
		}

		// Scalar values — must be equal
		if reflect.DeepEqual(existing, srcVal) {
			continue
		}

		return jmaperror.UnsupportedFilter("conflicting values for filter property: " + key)
	}
	return nil
}
