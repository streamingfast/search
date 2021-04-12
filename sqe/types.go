package sqe

import (
	"context"
	"fmt"
)

type Visitor interface {
	Visit_And(ctx context.Context, expr *AndExpression) error
	Visit_Or(ctx context.Context, expr *OrExpression) error
	Visit_Parenthesis(ctx context.Context, expr *ParenthesisExpression) error
	Visit_Not(ctx context.Context, expr *NotExpression) error
	Visit_SearchTerm(ctx context.Context, expr *SearchTerm) error
}

type Expression interface {
	Visit(ctx context.Context, visitor Visitor) error
}

type AndExpression struct {
	Children []Expression
}

func (e *AndExpression) Visit(ctx context.Context, visitor Visitor) error {
	return visitor.Visit_And(ctx, e)
}

type OrExpression struct {
	Children []Expression
}

func (e *OrExpression) Visit(ctx context.Context, visitor Visitor) error {
	return visitor.Visit_Or(ctx, e)
}

type ParenthesisExpression struct {
	Child Expression
}

func (e *ParenthesisExpression) Visit(ctx context.Context, visitor Visitor) error {
	return visitor.Visit_Parenthesis(ctx, e)
}

type NotExpression struct {
	Child Expression
}

func (e *NotExpression) Visit(ctx context.Context, visitor Visitor) error {
	return visitor.Visit_Not(ctx, e)
}

type SearchTerm struct {
	Field string
	Value *StringLiteral
}

func (e *SearchTerm) Visit(ctx context.Context, visitor Visitor) error {
	return visitor.Visit_SearchTerm(ctx, e)
}

func (e *SearchTerm) SetValue(in string) {
	e.Value.Value = in
}

type StringLiteral struct {
	Value       string
	QuotingChar string
}

func (e *StringLiteral) Literal() string {
	return e.Value
}

func (e *StringLiteral) String() string {
	if e.QuotingChar != "" {
		return fmt.Sprintf("%s%s%s", e.QuotingChar, e.Value, e.QuotingChar)
	}

	return e.Value
}
