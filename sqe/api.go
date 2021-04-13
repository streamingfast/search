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

	visitor := NewDepthFirstVisitor(nil, onExpression)
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
		v, ok := expr.(*SearchTerm)
		if !ok {
			return nil
		}

		switch w := v.Value.(type) {
		case *StringLiteral:
			if err := transformer.Transform(v.Field, w); err != nil {
				return fmt.Errorf("field %q transformation failed: %s", v.Field, err)
			}
		case *StringsList:
			for i, value := range w.Values {
				if err := transformer.Transform(v.Field, value); err != nil {
					return fmt.Errorf("field %q list item at index %d transformation failed: %s", v.Field, i, err)
				}
			}
		}

		return nil
	}

	visitor := NewDepthFirstVisitor(nil, onExpression)
	return expr.Visit(context.Background(), visitor)
}
