package sqe

import (
	"context"
	"fmt"
	"strings"
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

func andExpr(children ...Expression) *AndExpression {
	return &AndExpression{Children: children}
}

func (e *AndExpression) Visit(ctx context.Context, visitor Visitor) error {
	return visitor.Visit_And(ctx, e)
}

type OrExpression struct {
	Children []Expression
}

func orExpr(children ...Expression) *OrExpression {
	return &OrExpression{Children: children}
}

func (e *OrExpression) Visit(ctx context.Context, visitor Visitor) error {
	return visitor.Visit_Or(ctx, e)
}

type ParenthesisExpression struct {
	Child Expression
}

func parensExpr(expr Expression) *ParenthesisExpression {
	return &ParenthesisExpression{Child: expr}
}

func (e *ParenthesisExpression) Visit(ctx context.Context, visitor Visitor) error {
	return visitor.Visit_Parenthesis(ctx, e)
}

type NotExpression struct {
	Child Expression
}

func notExpr(expr Expression) *NotExpression {
	return &NotExpression{Child: expr}
}

func (e *NotExpression) Visit(ctx context.Context, visitor Visitor) error {
	return visitor.Visit_Not(ctx, e)
}

type SearchTerm struct {
	Field string
	Value SearchTermValue
}

func termExpr(field string, value interface{}) *SearchTerm {
	var termValue SearchTermValue
	switch v := value.(type) {
	case string:
		termValue = stringLiteral(v)
	case []string:
		termValue = stringsList(v...)
	case fmt.Stringer:
		termValue = stringLiteral(v.String())
	default:
		panic(fmt.Errorf("unable to infer term expr value from type %T", v))
	}

	return &SearchTerm{Field: field, Value: termValue}
}

type SearchTermValue interface {
	isValue() bool
	String() string
}

func (e *SearchTerm) Visit(ctx context.Context, visitor Visitor) error {
	return visitor.Visit_SearchTerm(ctx, e)
}

func (e *SearchTerm) SetValue(in SearchTermValue) {
	e.Value = in
}

func (e *SearchTerm) SetStringLiteralValue(in string) {
	e.Value = stringLiteral(in)
}

type StringLiteral struct {
	Value       string
	QuotingChar string
}

const restrictedLiteralChars = `'":,-()[] ` + "\n" + "\t"

func stringLiteral(in string) *StringLiteral {
	stringLiteral := &StringLiteral{Value: in}
	if strings.ContainsAny(in, restrictedLiteralChars) {
		stringLiteral.QuotingChar = "\""
	}

	return stringLiteral
}

func (e *StringLiteral) isValue() bool {
	return true
}

func (e *StringLiteral) Literal() string {
	return e.Value
}

func (e *StringLiteral) SetValue(value string) {
	e.Value = value
}

func (e *StringLiteral) String() string {
	if e.QuotingChar != "" {
		return fmt.Sprintf("%s%s%s", e.QuotingChar, e.Value, e.QuotingChar)
	}

	return e.Value
}

type StringsList struct {
	Values []*StringLiteral
}

func stringsList(elements ...string) *StringsList {
	values := make([]*StringLiteral, len(elements))
	for i, element := range elements {
		values[i] = stringLiteral(element)
	}

	return &StringsList{Values: values}
}

func (e *StringsList) isValue() bool {
	return true
}

func (e *StringsList) String() string {
	if len(e.Values) <= 0 {
		return "[]"
	}

	stringValues := make([]string, len(e.Values))
	for i, value := range e.Values {
		stringValues[i] = value.String()
	}

	return "[" + strings.Join(stringValues, ", ") + "]"
}
