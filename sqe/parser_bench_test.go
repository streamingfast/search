package sqe

import (
	"context"
	"strings"
	"testing"
)

func BenchmarkParseExpression(b *testing.B) {
	tests := []struct {
		name string
		sqe  string
	}{
		{"single term", "action:data"},

		// Those are kind of standard query that are parsed quite often
		{"triple and term", "receiver:eosio action:data data.from:specificacct"},
		{"multiple and term", "action:data data.from:bobby data.to:'action' data.expected:string"},
		{"multiple and/or term", "action:data (data.from:bobby OR data.from:bobby) (data.to:'action' OR data.expected:string) data.to:'action' data.expected:string"},

		// Some convoluted big ORs list
		{"big ORs list 100", buildFromOrToList(100)},
		{"big ORs list 1_000", buildFromOrToList(1000)},

		// Replacement for big ORs list where `field: [...]` can be used
		{"big strings list 100", buildStringsList(100)},
		{"big strings list 1_000", buildStringsList(1000)},
		{"big strings list 10_000", buildStringsList(10000)},
		{"big strings list 100_000", buildStringsList(100000)},
		{"big strings list 1_000_000", buildStringsList(1000000)},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			setupBench(b)
			for n := 0; n < b.N; n++ {
				_, err := Parse(context.Background(), test.sqe)
				if err != nil {
					b.Error(err)
					b.FailNow()
				}
			}
		})
	}
}

func buildFromOrToList(count int) string {
	var elements []string

	// The count is divided by 2 since we add 2 addresses per iteration
	for i := 0; i < count/2; i++ {
		elements = append(elements, "from:0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "to:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	}

	return "(" + strings.Join(elements, " OR ") + ")"
}

func buildStringsList(count int) string {
	var elements []string

	// The count is divided by 2 since we add 2 elements per iteration
	for i := 0; i < count; i++ {
		elements = append(elements, "andy", "amuchbiggeramuchbiggermuchmuchbiggerstring")
	}

	return "from:[" + strings.Join(elements, ", ") + "]"
}

func setupBench(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
}
