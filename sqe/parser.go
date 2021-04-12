package sqe

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
)

func Parse(input string) (expr Expression, err error) {
	parser, err := NewParser(bytes.NewBufferString(input))
	if err != nil {
		return nil, fmt.Errorf("new parser: %w", err)
	}

	return parser.Parse()
}

type Parser struct {
	ctx context.Context
	l   *lexer

	lookForRightParenthesis uint
}

func NewParser(reader io.Reader) (*Parser, error) {
	lexer, err := newLexer(reader)
	if err != nil {
		return nil, err
	}

	return &Parser{
		ctx: context.Background(),
		l:   lexer,
	}, nil
}

func (p *Parser) Parse() (out Expression, err error) {
	defer func() {
		recoveredErr := recover()
		if recoveredErr == nil {
			return
		}

		switch v := recoveredErr.(type) {
		case error:
			err = fmt.Errorf("unexpected error occurred while parsing SQE expression: %w", v)
		case string:
			err = fmt.Errorf("unexpected error occurred while parsing SQE expression: %s", v)
		case fmt.Stringer:
			err = fmt.Errorf("unexpected error occurred while parsing SQE expression: %s", v)
		default:
			err = fmt.Errorf("unexpected error occurred while parsing SQE expression: %v", v)
		}
	}()

	return p.parseExpression()
}

func (p *Parser) parseExpression() (Expression, error) {
	left, err := p.parseUnaryExpression()
	if err != nil {
		return nil, err
	}

	for {
		p.l.skipSpaces()
		next, err := p.l.Peek(0)
		if err != nil {
			return nil, err
		}

		// If we reached end of file, we have finished our job
		if next.EOF() {
			return left, nil
		}

		// If we reached right parenthesis, check if we were expecting one
		if p.l.isRightParenthesis(next) {
			if p.lookForRightParenthesis == 0 {
				return nil, parserError("unexpected right parenthesis, expected right hand side expression or end of input", next.Pos)
			}

			// We were expecting one, we finished our job for this part, decrement will be done at parsing site
			return left, nil
		}

		isImplicitAnd := true
		if p.l.isBinaryOperator(next) {
			isImplicitAnd = false
			p.l.mustLexNext()
			p.l.skipSpaces()
		}

		// This implements precedence order between `&&` and `||`. A `&&` is parsed with the smallest
		// next unit so it takes precedences while `||` parse with the longuest possibility.
		parser := p.parseUnaryExpression
		if p.l.isOrOperator(next) {
			parser = p.parseExpression
		}

		right, err := parser()

		switch {
		case isImplicitAnd || p.l.isAndOperator(next):
			if err != nil {
				if isImplicitAnd {
					return nil, fmt.Errorf("missing expression after implicit 'and' clause: %w", err)
				}

				return nil, fmt.Errorf("missing expression after 'and' clause: %w", err)
			}

			if v, ok := left.(*AndExpression); ok {
				v.Children = append(v.Children, right)
			} else {
				left = &AndExpression{Children: []Expression{left, right}}
			}

		case p.l.isOrOperator(next):
			if err != nil {
				return nil, fmt.Errorf("missing expression after 'or' clause: %w", err)
			}

			// It's impossible to coascle `||` expressions since they are recursive
			left = &OrExpression{Children: []Expression{left, right}}

		default:
			if err != nil {
				return nil, fmt.Errorf("unable to parse right hand side expression: %w", err)
			}

			return nil, parserError(fmt.Sprintf("token type %s is not valid binary right hand side expression", p.l.getTokenType(next)), next.Pos)
		}
	}
}

func (p *Parser) parseSearchTerm() (Expression, error) {
	fieldNameToken := p.l.mustLexNext()

	p.l.skipSpaces()

	colonToken, err := p.l.Next()
	if err != nil {
		return nil, err
	}

	if colonToken.EOF() {
		return nil, parserError("expecting colon after search field, got end of input", fieldNameToken.Pos)
	}

	if !p.l.isColon(colonToken) {
		return nil, parserError(fmt.Sprintf("expecting colon after search field, got %s", p.l.getTokenType(colonToken)), colonToken.Pos)
	}

	p.l.skipSpaces()

	token, err := p.l.Peek(0)
	if err != nil {
		return nil, err
	}

	if token.EOF() {
		return nil, parserError("expecting search value after field, got end of input", token.Pos)
	}

	var searchValue *StringLiteral

	switch {
	case p.l.isName(token):
		searchValue = &StringLiteral{
			Value: p.l.mustLexNext().String(),
		}
	case p.l.isQuoting(token):
		literal, err := p.parseQuotedString()
		if err != nil {
			return nil, err
		}

		searchValue = literal
	default:
		return nil, parserError(fmt.Sprintf("expecting search value after colon, got %s", p.l.getTokenType(token)), token.Pos)
	}

	return &SearchTerm{
		Field: fieldNameToken.String(),
		Value: searchValue,
	}, nil
}

func (p *Parser) parseUnaryExpression() (Expression, error) {
	p.l.skipSpaces()

	token, err := p.l.Peek(0)
	if err != nil {
		return nil, err
	}

	if token.EOF() {
		return nil, parserError("expected a search term, minus sign or left parenthesis, got end of input", token.Pos)
	}

	switch {
	case p.l.isName(token):
		return p.parseSearchTerm()
	case p.l.isLeftParenthesis(token):
		return p.parseParenthesisExpression()
	case p.l.isNotOperator(token):
		return p.parseNotExpression()
	default:
		return nil, parserError(fmt.Sprintf("expected a search term, minus sign or left parenthesis, got %s", p.l.getTokenType(token)), token.Pos)
	}
}

func (p *Parser) parseParenthesisExpression() (Expression, error) {
	// Consume left parenthesis
	openingParenthesis := p.l.mustLexNext()
	p.lookForRightParenthesis++

	child, err := p.parseExpression()
	if err != nil {
		return nil, fmt.Errorf("invalid expression after opening parenthesis: %w", err)
	}

	p.l.skipSpaces()
	token, err := p.l.Next()
	if err != nil {
		return nil, err
	}

	if token.EOF() {
		return nil, parserError("expecting closing parenthesis, got end of input", openingParenthesis.Pos)
	}

	if !p.l.isRightParenthesis(token) {
		return nil, parserError(fmt.Sprintf("expecting closing parenthesis after expression, got %s", p.l.getTokenType(token)), token.Pos)
	}

	p.lookForRightParenthesis--
	return &ParenthesisExpression{child}, nil
}

func (p *Parser) parseNotExpression() (Expression, error) {
	// Consume minus sign
	p.l.mustLexNext()

	child, err := p.parseUnaryExpression()
	if err != nil {
		return nil, fmt.Errorf("invalid expression after minus sign: %w", err)
	}

	return &NotExpression{child}, nil
}

func (p *Parser) parseQuotedString() (*StringLiteral, error) {
	startQuoting := p.l.mustLexNext()

	builder := &strings.Builder{}
	for {
		token, err := p.l.Next()
		if err != nil {
			return nil, err
		}

		if token.EOF() {
			return nil, parserError(fmt.Sprintf("expecting closing quoting char %q, got end of input", startQuoting.Value), startQuoting.Pos)
		}

		if p.l.isQuoting(token) {
			value := builder.String()
			if value == "" {
				return nil, rangeParserError("an empty string is not valid", startQuoting.Pos, token.Pos)
			}

			return &StringLiteral{
				Value:       value,
				QuotingChar: startQuoting.Value,
			}, nil
		}

		builder.WriteString(token.Value)
	}
}
