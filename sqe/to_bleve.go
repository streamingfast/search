package sqe

import (
	"fmt"

	"github.com/blevesearch/bleve/search/query"
)

func expressionToBleve(expr Expression, depth int) query.Query {
	switch v := expr.(type) {
	case *SearchTerm:
		return searchTermToQuery(v)

	case *AndExpression:
		if isSearchTermsOnly(v.Children) {
			return newConjuncts(searchTermsAsExprToQuery(v.Children)...)
		}

		conjuncts := newConjuncts()
		for _, child := range v.Children {
			conjuncts.AddQuery(expressionToBleve(child, depth+1))
		}

		return conjuncts

	case *OrExpression:
		if isSearchTermsOnly(v.Children) {
			return newDisjuncts(searchTermsAsExprToQuery(v.Children)...)
		}

		disjuncts := newDisjuncts()
		for _, child := range v.Children {
			disjuncts.AddQuery(expressionToBleve(child, depth+1))
		}

		return disjuncts

	case *ParenthesisExpression:
		return expressionToBleve(v.Child, depth+1)

	case *NotExpression:
		childQuery := expressionToBleve(v.Child, depth+1)

		return query.NewBooleanQuery(nil, nil, []query.Query{childQuery})

	default:
		panic(fmt.Errorf("element of type %T is not handled correctly", v))
	}
}

func searchTermsAsExprToQuery(children []Expression) (out []query.Query) {
	out = make([]query.Query, len(children))
	for i, child := range children {
		// It's guaranteed to be called with only child of type *SearchTerm
		out[i] = searchTermToQuery(child.(*SearchTerm))
	}

	return
}

func searchTermToQuery(e *SearchTerm) query.Query {
	switch v := e.Value.(type) {
	case *StringLiteral:
		return stringLiteralToQuery(e.Field, v)

	case *StringsList:
		if len(v.Values) <= 0 {
			// The idea here is to match absolutely nothing, I did not find yet what is best way to create this in Bleve, using this literal is probably good enough for now
			term := query.NewTermQuery("__!{+}!__")
			term.SetField(e.Field)

			return term
		}

		children := make([]query.Query, len(v.Values))
		for i, child := range v.Values {
			children[i] = stringLiteralToQuery(e.Field, child)
		}

		return newDisjuncts(children...)

	default:
		panic(fmt.Errorf("the SQE AST node of type %T is not handled properly when converting SQE to Bleve query", v))
	}
}

func stringLiteralToQuery(field string, literal *StringLiteral) query.Query {
	value := literal.Literal()
	// If the search term is non-quoted and it's either true or false, use a BoolQuery
	if literal.QuotingChar == "" {
		if value == "true" {
			return &query.BoolFieldQuery{FieldVal: field, Bool: true}
		}

		if value == "false" {
			return &query.BoolFieldQuery{FieldVal: field, Bool: false}
		}
	}

	return &query.TermQuery{FieldVal: field, Term: value}
}

func isSearchTermsOnly(children []Expression) bool {
	for _, child := range children {
		if _, ok := child.(*SearchTerm); !ok {
			return false
		}
	}

	return true
}

type collectionQuery interface {
	query.Query
	AddQuery(aq ...query.Query)
}

func newConjuncts(children ...query.Query) collectionQuery {
	return query.NewConjunctionQuery(children)
}

func newDisjuncts(children ...query.Query) collectionQuery {
	disjuncts := query.NewDisjunctionQuery(children)
	disjuncts.SetMin(1)

	return disjuncts
}
