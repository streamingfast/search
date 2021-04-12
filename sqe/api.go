package sqe

import (
	"context"
	"fmt"

	"github.com/blevesearch/bleve/search/query"
)

func ExpressionToBleve(expr Expression) query.Query {
	return expressionToBleve(expr, 0)
}

// FindAllFieldNames returns all used field names in the AST. There
// is **NO** ordering on the elements, i.e. they might not come in the
// same order specified in the AST.
func ExtractAllFieldNames(expression Expression) (out []string) {
	uniqueFieldNames := map[string]bool{}
	onExpression := func(_ context.Context, expr Expression) error {
		if v, ok := expr.(*SearchTerm); ok {
			uniqueFieldNames[v.Field] = true
		}

		return nil
	}

	visitor := NewDepthFirstVisitor(onExpression)
	expression.Visit(context.Background(), visitor)

	i := 0
	out = make([]string, len(uniqueFieldNames))
	for fieldName := range uniqueFieldNames {
		out[i] = fieldName
		i++
	}

	return
}

func TransformExpression(expr Expression, transformer FieldTransformer) error {
	if transformer == nil {
		return nil
	}

	onExpression := func(_ context.Context, expr Expression) error {
		if v, ok := expr.(*SearchTerm); ok {
			if err := transformer.Transform(v); err != nil {
				return fmt.Errorf("field %q transformation failed: %s", v.Field, err)
			}
		}

		return nil
	}

	visitor := NewDepthFirstVisitor(onExpression)
	return expr.Visit(context.Background(), visitor)
}
