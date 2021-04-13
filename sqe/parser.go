package sqe

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
)

// MaxRecursionDeepness is the limit we impose on the number of direct ORs expression.
// It's possible to have more than that, just not in a single successive sequence or `1 or 2 or 3 ...`.
// This is to avoid first a speed problem where parsing start to be
const MaxRecursionDeepness = 2501

func Parse(ctx context.Context, input string) (expr Expression, err error) {
	parser, err := NewParser(bytes.NewBufferString(input))
	if err != nil {
		return nil, fmt.Errorf("new parser: %w", err)
	}

	return parser.Parse(ctx)
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

func (p *Parser) Parse(ctx context.Context) (out Expression, err error) {
	defer func() {
		recoveredErr := recover()
		if recoveredErr == nil {
			return
		}

		switch v := recoveredErr.(type) {
		case *ParseError:
			err = v
		case error:
			err = fmt.Errorf("unexpected error occurred while parsing SQE expression: %w", v)
		case string, fmt.Stringer:
			err = fmt.Errorf("unexpected error occurred while parsing SQE expression: %s", v)
		default:
			err = fmt.Errorf("unexpected error occurred while parsing SQE expression: %v", v)
		}
	}()

	rootExpr, err := p.parseExpression(0)
	if err != nil {
		return nil, err
	}

	return optimizeExpression(ctx, rootExpr), nil
}

func (p *Parser) parseExpression(depth int) (Expression, error) {
	if depth >= MaxRecursionDeepness {
		// This is a small hack, the panic is trapped at the public API `Parse` method. We do it with a panic
		// to avoid the really deep wrapping of error that would happen if we returned right away. A test ensure
		// that this behavior works as expected.
		panic(parserError("expression is too long, too much ORs or parenthesis expressions", p.l.peekPos()))
	}

	left, err := p.parseUnaryExpression(depth)
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
		depthIncrease := 0
		if p.l.isOrOperator(next) {
			parser = p.parseExpression
			depthIncrease = 1
		}

		right, err := parser(depth + depthIncrease)

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

func (p *Parser) parseUnaryExpression(depth int) (Expression, error) {
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
		return p.parseParenthesisExpression(depth)
	case p.l.isNotOperator(token):
		return p.parseNotExpression(depth)
	default:
		return nil, parserError(fmt.Sprintf("expected a search term, minus sign or left parenthesis, got %s", p.l.getTokenType(token)), token.Pos)
	}
}

func (p *Parser) parseParenthesisExpression(depth int) (Expression, error) {
	// Consume left parenthesis
	openingParenthesis := p.l.mustLexNext()
	p.lookForRightParenthesis++

	child, err := p.parseExpression(depth + 1)
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

func (p *Parser) parseNotExpression(depth int) (Expression, error) {
	// Consume minus sign
	p.l.mustLexNext()

	child, err := p.parseUnaryExpression(depth)
	if err != nil {
		return nil, fmt.Errorf("invalid expression after minus sign: %w", err)
	}

	return &NotExpression{child}, nil
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

	var searchValue SearchTermValue

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
	case p.l.isLeftSquareBracket(token):
		list, err := p.parseStringsList()
		if err != nil {
			return nil, err
		}

		searchValue = list
	default:
		return nil, parserError(fmt.Sprintf("expecting search value after colon, either a string, quoted string or strings list got %s", p.l.getTokenType(token)), token.Pos)
	}

	return &SearchTerm{
		Field: fieldNameToken.String(),
		Value: searchValue,
	}, nil
}

func (p *Parser) parseStringsList() (*StringsList, error) {
	// Consume left square bracket
	p.l.mustLexNext()

	list := &StringsList{}
	lookFor := "name"

	for {
		p.l.skipSpaces()
		token, err := p.l.Peek(0)
		if err != nil {
			return nil, err
		}

		if token.EOF() {
			return nil, parserError("expecting string value in list, got end of input, right square bracket ']' missing", token.Pos)
		}

		// If we reached the square bracket, we are done, consume characters and return accumulated list
		if p.l.isRightSquareBracket(token) {
			p.l.mustLexNext()
			return list, nil
		}

		if lookFor == "comma" {
			if !p.l.isComma(token) {
				return nil, parserError(fmt.Sprintf("expecting comma after string value item, got %T", p.l.getTokenType(token)), token.Pos)
			}

			// It's a comma, skip and change to look for name
			p.l.mustLexNext()
			lookFor = "name"
			continue
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
			return nil, parserError(fmt.Sprintf("expecting string value in list, either a string or quoted string got %s", p.l.getTokenType(token)), token.Pos)
		}

		list.Values = append(list.Values, searchValue)
		lookFor = "comma"
	}
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
