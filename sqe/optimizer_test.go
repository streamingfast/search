package sqe

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptimizer(t *testing.T) {
	tests := []struct {
		name     string
		expr     Expression
		expected string
	}{
		{
			"top_or_no_or_children",
			orExpr(termExpr("a", "1"), termExpr("a", "2")),
			`[a:1 || a:2]`,
		},
		{
			"top_or_single_or_children",
			orExpr(orExpr(termExpr("a", "1"), termExpr("a", "2")), termExpr("b", "2")),
			`[a:1 || a:2 || b:2]`,
		},
		{
			"top_or_multiple_or_children",
			orExpr(
				orExpr(termExpr("a", "1"), termExpr("a", "2")),
				orExpr(termExpr("c", "1"), termExpr("c", "2")),
			),
			`[a:1 || a:2 || c:1 || c:2]`,
		},
		{
			"top_or_mixed_multiple_or_children",
			orExpr(
				termExpr("before", "2"),
				orExpr(termExpr("a", "1"), termExpr("a", "2")),
				andExpr(termExpr("middle", "1"), termExpr("middle", "2")),
				orExpr(termExpr("c", "1"), termExpr("c", "2")),
				notExpr(termExpr("after", "3")),
			),
			`[before:2 || a:1 || a:2 || <middle:1 && middle:2> || c:1 || c:2 || !after:3]`,
		},

		{
			"or_in_not_multiple_or_children",
			notExpr(
				orExpr(
					orExpr(termExpr("a", "1"), termExpr("a", "2")),
					orExpr(termExpr("c", "1"), termExpr("c", "2")),
				),
			),
			`![a:1 || a:2 || c:1 || c:2]`,
		},
		{
			"or_in_parens_multiple_or_children",
			parensExpr(
				orExpr(
					orExpr(termExpr("a", "1"), termExpr("a", "2")),
					orExpr(termExpr("c", "1"), termExpr("c", "2")),
				),
			),
			`([a:1 || a:2 || c:1 || c:2])`,
		},

		{
			"multi_level_nested_only_or",
			orExpr(
				orExpr(
					orExpr(
						termExpr("l3", "a1"),
						orExpr(termExpr("l4", "a1"), termExpr("l4", "a2")),
					),
					orExpr(
						orExpr(termExpr("l4", "b1"), termExpr("l4", "b2")),
						termExpr("l3", "b1"),
					),
					orExpr(
						orExpr(termExpr("l4", "c1"), termExpr("l4", "c2")),
						orExpr(termExpr("l4", "d1"), termExpr("l4", "d2")),
					),
				),
			),
			`[l3:a1 || l4:a1 || l4:a2 || l4:b1 || l4:b2 || l3:b1 || l4:c1 || l4:c2 || l4:d1 || l4:d2]`,
		},

		{
			"multi_level_nested_mixed_or",
			orExpr(
				orExpr(
					andExpr(
						termExpr("l3", "a1"),
						notExpr(orExpr(termExpr("l4", "a1"), termExpr("l4", "a2"))),
					),
					orExpr(
						orExpr(termExpr("l4", "b1"), termExpr("l4", "b2")),
						termExpr("l3", "b1"),
					),
					orExpr(
						orExpr(termExpr("l4", "c1"), termExpr("l4", "c2")),
						parensExpr(orExpr(termExpr("l4", "d1"), termExpr("l4", "d2"))),
					),
				),
				andExpr(
					termExpr("l2", "e1"),
					orExpr(termExpr("l3", "f1"), termExpr("l3", "f2")),
				),
			),
			`[<l3:a1 && ![l4:a1 || l4:a2]> || l4:b1 || l4:b2 || l3:b1 || l4:c1 || l4:c2 || ([l4:d1 || l4:d2]) || <l2:e1 && [l3:f1 || l3:f2]>]`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimized := optimizeExpression(context.Background(), test.expr)
			assert.Equal(t, test.expected, expressionToString(optimized), "Invalid optimization for %q", test.name)
		})
	}
}
