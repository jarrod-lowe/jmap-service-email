package filter

import (
	"testing"

	"github.com/jarrod-lowe/jmap-service-libs/plugincontract"
)

func TestFlatConditionPassthrough(t *testing.T) {
	input := plugincontract.Args{
		"inMailbox":  "inbox-1",
		"hasKeyword": "$seen",
	}

	result, err := FlattenFilter(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.StringOr("inMailbox", "") != "inbox-1" {
		t.Errorf("inMailbox = %q, want %q", result.StringOr("inMailbox", ""), "inbox-1")
	}
	if result.StringOr("hasKeyword", "") != "$seen" {
		t.Errorf("hasKeyword = %q, want %q", result.StringOr("hasKeyword", ""), "$seen")
	}
}

func TestANDWithTwoCompatibleConditions(t *testing.T) {
	input := plugincontract.Args{
		"operator": "AND",
		"conditions": []any{
			map[string]any{"inMailbox": "inbox-1"},
			map[string]any{"hasKeyword": "$seen"},
		},
	}

	result, err := FlattenFilter(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.StringOr("inMailbox", "") != "inbox-1" {
		t.Errorf("inMailbox = %q, want %q", result.StringOr("inMailbox", ""), "inbox-1")
	}
	if result.StringOr("hasKeyword", "") != "$seen" {
		t.Errorf("hasKeyword = %q, want %q", result.StringOr("hasKeyword", ""), "$seen")
	}
}

func TestNestedAND(t *testing.T) {
	input := plugincontract.Args{
		"operator": "AND",
		"conditions": []any{
			map[string]any{"inMailbox": "inbox-1"},
			map[string]any{
				"operator": "AND",
				"conditions": []any{
					map[string]any{"hasKeyword": "$seen"},
					map[string]any{"from": "alice"},
				},
			},
		},
	}

	result, err := FlattenFilter(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.StringOr("inMailbox", "") != "inbox-1" {
		t.Errorf("inMailbox = %q, want %q", result.StringOr("inMailbox", ""), "inbox-1")
	}
	if result.StringOr("hasKeyword", "") != "$seen" {
		t.Errorf("hasKeyword = %q, want %q", result.StringOr("hasKeyword", ""), "$seen")
	}
	if result.StringOr("from", "") != "alice" {
		t.Errorf("from = %q, want %q", result.StringOr("from", ""), "alice")
	}
}

func TestORWithSingleCondition(t *testing.T) {
	input := plugincontract.Args{
		"operator": "OR",
		"conditions": []any{
			map[string]any{"to": "jmap-test"},
		},
	}

	result, err := FlattenFilter(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.StringOr("to", "") != "jmap-test" {
		t.Errorf("to = %q, want %q", result.StringOr("to", ""), "jmap-test")
	}
}

func TestORWithMultipleConditions_Error(t *testing.T) {
	input := plugincontract.Args{
		"operator": "OR",
		"conditions": []any{
			map[string]any{"from": "alice"},
			map[string]any{"from": "bob"},
		},
	}

	_, err := FlattenFilter(input)
	if err == nil {
		t.Fatal("expected error for OR with multiple conditions")
	}
	if got := err.Error(); got != "unsupportedFilter: OR filter with multiple conditions is not supported" {
		t.Errorf("error = %q, want unsupportedFilter message about OR", got)
	}
}

func TestNOT_Error(t *testing.T) {
	input := plugincontract.Args{
		"operator": "NOT",
		"conditions": []any{
			map[string]any{"from": "alice"},
		},
	}

	_, err := FlattenFilter(input)
	if err == nil {
		t.Fatal("expected error for NOT operator")
	}
	if got := err.Error(); got != "unsupportedFilter: NOT filter is not supported" {
		t.Errorf("error = %q, want unsupportedFilter message about NOT", got)
	}
}

func TestConflictingInMailbox_Error(t *testing.T) {
	input := plugincontract.Args{
		"operator": "AND",
		"conditions": []any{
			map[string]any{"inMailbox": "inbox-1"},
			map[string]any{"inMailbox": "inbox-2"},
		},
	}

	_, err := FlattenFilter(input)
	if err == nil {
		t.Fatal("expected error for conflicting inMailbox values")
	}
	if got := err.Error(); got != "unsupportedFilter: conflicting values for filter property: inMailbox" {
		t.Errorf("error = %q, want unsupportedFilter message about conflict", got)
	}
}

func TestInMailboxOtherThanArrayUnion(t *testing.T) {
	input := plugincontract.Args{
		"operator": "AND",
		"conditions": []any{
			map[string]any{"inMailboxOtherThan": []any{"junk"}},
			map[string]any{"inMailboxOtherThan": []any{"trash"}},
		},
	}

	result, err := FlattenFilter(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ids, ok := result.StringSlice("inMailboxOtherThan")
	if !ok {
		t.Fatal("inMailboxOtherThan not found or not a string slice")
	}
	if len(ids) != 2 {
		t.Fatalf("inMailboxOtherThan length = %d, want 2", len(ids))
	}
	if ids[0] != "junk" || ids[1] != "trash" {
		t.Errorf("inMailboxOtherThan = %v, want [junk trash]", ids)
	}
}

func TestAercFilterExample(t *testing.T) {
	// Exact filter from aerc bug report:
	// {"operator":"AND","conditions":[{"inMailboxOtherThan":["junk","trash"]},{"operator":"OR","conditions":[{"to":"jmap-test"}]}]}
	input := plugincontract.Args{
		"operator": "AND",
		"conditions": []any{
			map[string]any{"inMailboxOtherThan": []any{"junk", "trash"}},
			map[string]any{
				"operator": "OR",
				"conditions": []any{
					map[string]any{"to": "jmap-test"},
				},
			},
		},
	}

	result, err := FlattenFilter(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ids, ok := result.StringSlice("inMailboxOtherThan")
	if !ok {
		t.Fatal("inMailboxOtherThan not found or not a string slice")
	}
	if len(ids) != 2 || ids[0] != "junk" || ids[1] != "trash" {
		t.Errorf("inMailboxOtherThan = %v, want [junk trash]", ids)
	}
	if result.StringOr("to", "") != "jmap-test" {
		t.Errorf("to = %q, want %q", result.StringOr("to", ""), "jmap-test")
	}
}

func TestEmptyConditionsArray(t *testing.T) {
	input := plugincontract.Args{
		"operator":   "AND",
		"conditions": []any{},
	}

	result, err := FlattenFilter(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got %v", result)
	}
}

func TestInvalidOperatorValue_Error(t *testing.T) {
	input := plugincontract.Args{
		"operator": "XOR",
		"conditions": []any{
			map[string]any{"from": "alice"},
		},
	}

	_, err := FlattenFilter(input)
	if err == nil {
		t.Fatal("expected error for invalid operator")
	}
	if got := err.Error(); got != "unsupportedFilter: unknown filter operator: XOR" {
		t.Errorf("error = %q, want unsupportedFilter message about unknown operator", got)
	}
}

func TestSameScalarValueNonConflict(t *testing.T) {
	// Same key with same value across AND branches is not a conflict
	input := plugincontract.Args{
		"operator": "AND",
		"conditions": []any{
			map[string]any{"inMailbox": "inbox-1"},
			map[string]any{"inMailbox": "inbox-1"},
		},
	}

	result, err := FlattenFilter(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.StringOr("inMailbox", "") != "inbox-1" {
		t.Errorf("inMailbox = %q, want %q", result.StringOr("inMailbox", ""), "inbox-1")
	}
}

func TestIsFilterOperator(t *testing.T) {
	if IsFilterOperator(plugincontract.Args{"inMailbox": "x"}) {
		t.Error("flat condition should not be a FilterOperator")
	}
	if !IsFilterOperator(plugincontract.Args{"operator": "AND", "conditions": []any{}}) {
		t.Error("args with operator key should be a FilterOperator")
	}
}
