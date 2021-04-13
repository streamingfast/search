package sqe

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	lex "github.com/alecthomas/participle/lexer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const ValidateOnlyThatItParses = "!__valiateOnlyThatItParses__!"

func TestParser(t *testing.T) {
	tests := []struct {
		name        string
		sqe         string
		expected    string
		expectedErr error
	}{
		{
			"single_search_term",
			`action:transfer`,
			`action:transfer`,
			nil,
		},
		{
			"single_search_term_space_before_colon",
			`action :transfer`,
			`action:transfer`,
			nil,
		},
		{
			"single_search_term_space_after_colon",
			`action: transfer`,
			`action:transfer`,
			nil,
		},
		{
			"single_search_term_space_between_colon",
			`action : transfer`,
			`action:transfer`,
			nil,
		},
		{
			"single_search_term_multi_spaces",
			"action   \t : transfer",
			`action:transfer`,
			nil,
		},
		{
			"single_search_term_with_dot",
			`data.name:transfer`,
			`data.name:transfer`,
			nil,
		},
		{
			"double_quoted_string",
			`action:"test:value OR value AND other   	( 10 )!"`,
			`action:"test:value OR value AND other   	( 10 )!"`,
			nil,
		},
		{
			"double_quoted_string_multi_spaces",
			`action :   "  test:value OR value AND other   	( 10 )!"`,
			`action:"  test:value OR value AND other   	( 10 )!"`,
			nil,
		},
		{
			"double_quoted_string_with_minus_sign",
			`action:"eosio.token-open"`,
			`action:"eosio.token-open"`,
			nil,
		},

		{
			"single_quoted_string",
			`action:'test:value OR value AND other   	( 10 )!'`,
			`action:'test:value OR value AND other   	( 10 )!'`,
			nil,
		},
		{
			"single_quoted_string_multi_spaces",
			`action :   '  test:value OR value AND other   	( 10 )!'`,
			`action:'  test:value OR value AND other   	( 10 )!'`,
			nil,
		},

		{
			"search_term_strings_list_empty",
			`a: [   ] `,
			`a:[]`,
			nil,
		},
		{
			"search_term_strings_list_single_element",
			`a: [  value ] `,
			`a:[value]`,
			nil,
		},
		{
			"search_term_strings_list_multi_elements",
			`a: [  value, second ] `,
			`a:[value, second]`,
			nil,
		},
		{
			"search_term_strings_list_multi_elements_trailing_comma",
			`a: [  value, second, ] `,
			`a:[value, second]`,
			nil,
		},
		{
			"search_term_strings_list_string_single_quoted",
			`a: [  value, second, 'third, value' ] `,
			`a:[value, second, 'third, value']`,
			nil,
		},
		{
			"search_term_strings_list_string_double_quoted",
			`a: [  value, second, "third, value" ] `,
			`a:[value, second, "third, value"]`,
			nil,
		},

		{
			"top_level_single_and_implicit",
			`action:one field:two`,
			"<action:one && field:two>",
			nil,
		},
		{
			"top_level_single_and_implicit_double_quotes",
			`action:"one" field:two`,
			`<action:"one" && field:two>`,
			nil,
		},
		{
			"top_level_single_and",
			`action:one && field:two`,
			"<action:one && field:two>",
			nil,
		},
		{
			"top_level_single_and_legacy",
			`action:one AND field:two`,
			"<action:one && field:two>",
			nil,
		},

		{
			"top_level_single_or",
			`action:one || field:two`,
			"[action:one || field:two]",
			nil,
		},
		{
			"top_level_single_or_legacy",
			`action:one OR field:two`,
			"[action:one || field:two]",
			nil,
		},

		{
			"top_level_parenthesis_single_term",
			`(action:one)`,
			`(action:one)`,
			nil,
		},
		{
			"top_level_parenthesis_and_term",
			`(action:one && field:two)`,
			`(<action:one && field:two>)`,
			nil,
		},
		{
			"top_level_parenthesis_and_term_double_quote",
			`(action:one && field:"two")`,
			`(<action:one && field:"two">)`,
			nil,
		},
		{
			"top_level_parenthesis_or_term",
			`(action:one || field:two)`,
			`([action:one || field:two])`,
			nil,
		},
		{
			"top_level_parenthesis_or_term_with_double_quotes",
			`(action:  "one"   || field:two)`,
			`([action:"one" || field:two])`,
			nil,
		},
		{
			"top_level_parenthesis_with_spaces",
			` ( action:one || field:two   )  `,
			`([action:one || field:two])`,
			nil,
		},
		{
			"top_level_parenthesis_with_both_not",
			` ( -action:one OR -field:two   )  `,
			`([!action:one || !field:two])`,
			nil,
		},

		{
			"top_level_not_term",
			`- action:one`,
			`!action:one`,
			nil,
		},
		{
			"top_level_not_parenthesis",
			`- ( action:one)`,
			`!(action:one)`,
			nil,
		},
		{
			"top_level_not_parenthesis_or",
			`- ( action:one OR value:two)`,
			`!([action:one || value:two])`,
			nil,
		},

		{
			"top_level_implicit_and_with_left_not",
			` - action:two action:one`,
			`<!action:two && action:one>`,
			nil,
		},
		{
			"top_level_implicit_and_with_right_not",
			`action:two -action:one`,
			`<action:two && !action:one>`,
			nil,
		},
		{
			"top_level_implicit_and_both_not",
			`-action:two -action:one`,
			`<!action:two && !action:one>`,
			nil,
		},
		{
			"top_level_and_with_left_not",
			` - action:two && action:one`,
			`<!action:two && action:one>`,
			nil,
		},
		{
			"top_level_and_with_right_not",
			`action:two &&   -action:one`,
			`<action:two && !action:one>`,
			nil,
		},
		{
			"top_level_and_both_not",
			`-action:two &&   -action:one`,
			`<!action:two && !action:one>`,
			nil,
		},
		{
			"top_level_or_with_left_not",
			` - action:two || action:one`,
			`[!action:two || action:one]`,
			nil,
		},
		{
			"top_level_or_with_right_not",
			`action:two ||   -action:one`,
			`[action:two || !action:one]`,
			nil,
		},
		{
			"top_level_or_with_both_not",
			`-action:two ||   -action:one`,
			`[!action:two || !action:one]`,
			nil,
		},
		{
			"top_level_legacy_or_with_both_not",
			`-action:two OR   -action:one`,
			`[!action:two || !action:one]`,
			nil,
		},

		{
			"top_level_multi_and",
			`a:1 b:2 c:3 d:4`,
			`<a:1 && b:2 && c:3 && d:4>`,
			nil,
		},
		{
			"top_level_multi_or",
			`a:1 OR b:2 OR c:3 OR d:4`,
			`[a:1 || b:2 || c:3 || d:4]`,
			nil,
		},

		{
			"precedence_and_or",
			`a:1 b:2 OR c:3`,
			`[<a:1 && b:2> || c:3]`,
			nil,
		},
		{
			"precedence_or_and",
			`a:1 OR b:2 c:3`,
			`[a:1 || <b:2 && c:3>]`,
			nil,
		},
		{
			"precedence_and_or_and",
			`a:1 b:2 OR c:3 d:4`,
			`[<a:1 && b:2> || <c:3 && d:4>]`,
			nil,
		},
		{
			"precedence_and_and_or",
			`a:1 b:2 c:3 OR d:4`,
			`[<a:1 && b:2 && c:3> || d:4]`,
			nil,
		},
		{
			"precedence_not_and_or",
			`-a:1 b:2 OR c:3`,
			`[<!a:1 && b:2> || c:3]`,
			nil,
		},
		{
			"precedence_parenthesis_not_and_or",
			`-a:1 (b:2 OR c:3)`,
			`<!a:1 && ([b:2 || c:3])>`,
			nil,
		},
		{
			"precedence_parenthesis_and_or_and",
			`a:1 (b:2 OR c:3) d:4`,
			`<a:1 && ([b:2 || c:3]) && d:4>`,
			nil,
		},
		{
			"precedence_parenthesis_and_or",
			`a:1 (b:2 OR c:3)`,
			`<a:1 && ([b:2 || c:3])>`,
			nil,
		},

		{
			"ported_big_example",
			`data.from:"eos" (action:transfer OR action:issue OR action:matant) data.to:from data.mama:to`,
			`<data.from:"eos" && ([action:transfer || action:issue || action:matant]) && data.to:from && data.mama:to>`,
			nil,
		},
		{
			"ported_with_newlines",
			"(a:1 OR\n b:2)",
			`([a:1 || b:2])`,
			nil,
		},

		{
			"depthness_100_ors",
			buildFromOrToList(100),
			ValidateOnlyThatItParses,
			nil,
		},
		{
			"depthness_1_000_ors",
			buildFromOrToList(1000),
			ValidateOnlyThatItParses,
			nil,
		},
		{
			"depthness_2_500_ors",
			buildFromOrToList(2500),
			ValidateOnlyThatItParses,
			nil,
		},

		{
			"error_missing_literal_after_colon",
			`a:  `,
			"",
			&ParseError{"expecting search value after field, got end of input", pos(1, 4, 5)},
		},
		{
			"error_missing_expresssion_after_not",
			`a:1 - `,
			"",
			fmt.Errorf("missing expression after implicit 'and' clause: %w",
				fmt.Errorf("invalid expression after minus sign: %w",
					&ParseError{"expected a search term, minus sign or left parenthesis, got end of input", pos(1, 6, 7)},
				),
			),
		},
		{
			"error_missing_expression_after_and",
			`a:2 AND `,
			"",
			fmt.Errorf("missing expression after 'and' clause: %w",
				&ParseError{"expected a search term, minus sign or left parenthesis, got end of input", pos(1, 8, 9)},
			),
		},
		{
			"error_missing_expression_after_or",
			`a:2 OR `,
			"",
			fmt.Errorf("missing expression after 'or' clause: %w", &ParseError{"expected a search term, minus sign or left parenthesis, got end of input", pos(1, 7, 8)}),
		},
		{
			"error_unstarted_right_parenthesis",
			`a:1 )`,
			"",
			&ParseError{"unexpected right parenthesis, expected right hand side expression or end of input", pos(1, 4, 5)},
		},
		{
			"error_unclosed_over_left_parenthesis",
			`( a:1`,
			"",
			&ParseError{"expecting closing parenthesis, got end of input", pos(1, 0, 1)},
		},
		{
			"error_unstarted_right_square_bracket",
			`a: ]`,
			"",
			&ParseError{"expecting search value after colon, either a string, quoted string or strings list got RightSquareBracket", pos(1, 3, 4)},
		},
		{
			"error_unclosed_over_left_square_bracket",
			`a:[`,
			"",
			&ParseError{"expecting string value in list, got end of input, right square bracket ']' missing", pos(1, 3, 4)},
		},
		{
			"error_unstarted_random_right_square_bracket",
			`a:1 ]`,
			"",
			fmt.Errorf("missing expression after implicit 'and' clause: %w",
				&ParseError{"expected a search term, minus sign or left parenthesis, got RightSquareBracket", pos(1, 4, 5)},
			),
		},
		{
			"error_unclosed_random_over_left_square_bracket",
			`[ a:1`,
			"",
			&ParseError{"expected a search term, minus sign or left parenthesis, got LeftSquareBracket", pos(1, 0, 1)},
		},
		{
			"error_deepness_reached",
			buildFromOrToList(MaxRecursionDeepness + 1),
			"",
			&ParseError{"expression is too long, too much ORs or parenthesis expressions", pos(1, 101251, 101252)},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if os.Getenv("DEBUG") != "" {
				printTokens(t, test.sqe)
			}

			parser, err := NewParser(strings.NewReader(test.sqe))
			require.NoError(t, err)

			expression, err := parser.Parse(context.Background())
			require.Equal(t, test.expectedErr, err)

			if test.expectedErr == nil && err == nil && test.expected != ValidateOnlyThatItParses {
				assert.Equal(t, test.expected, expressionToString(expression), "Invalid parsing for SEQ %q", test.sqe)
			}
		})
	}
}

func TestParserFromBigQuery(t *testing.T) {
	bigQueryResultsFile := os.Getenv("SEARCH_SQE_BIGQUERY_RESULTS_FILE")
	if bigQueryResultsFile == "" {
		t.Skip("Environment variable SEARCH_SQE_BIGQUERY_RESULTS_FILE pointing to a JSON list in the form `[{\"query\":\"...\"}, ...]` must be specified to run this test")
		return
	}

	jsonFile, err := ioutil.ReadFile(os.Getenv("SEARCH_SQE_BIGQUERY_RESULTS_FILE"))
	require.NoError(t, err)

	type queryItem struct {
		Query string `json:"query"`
	}

	var queries []queryItem
	err = json.Unmarshal(jsonFile, &queries)
	require.NoError(t, err)

	for i, query := range queries {
		t.Run(fmt.Sprintf("element #%d - 'jq .[%d]'", i, i), func(t *testing.T) {
			parser, err := NewParser(strings.NewReader(query.Query))
			require.NoError(t, err)

			expression, err := parser.Parse(context.Background())
			require.NoError(t, err, "Parse failed for %q", query.Query)
			require.NotNil(t, expression)
		})
	}
}

func pos(line, offset, column int) lex.Position {
	return lex.Position{Filename: "", Line: line, Offset: offset, Column: column}
}

func printTokens(t *testing.T, input string) {
	lexer, err := lexerDefinition.Lex(strings.NewReader(input))
	require.NoError(t, err)

	tokens, err := lex.ConsumeAll(lexer)
	require.NoError(t, err)

	for _, token := range tokens {
		fmt.Print(token.GoString())
	}
}
