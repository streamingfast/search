package querylang

import (
	"strings"
	"testing"
)

func BenchmarkParseExpression(b *testing.B) {
	tests := []struct {
		name string
		sqe  string
	}{
		{"single term", "action:data"},
		{"multiple and term", "action:data data.from:bobby data.to:'action' data.expected:string"},
		{"multiple and/or term", "action:data (data.from:bobby OR data.from:bobby) (data.to:'action' OR data.expected:string) data.to:'action' data.expected:string"},
		{"big ORs list 100", buildFromOrToList(100)},
		{"big ORs list 1000", buildFromOrToList(1000)},
		{"big ORs list 10000", buildFromOrToList(10000)},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			setupBench(b)
			for n := 0; n < b.N; n++ {
				_, err := Parse(test.sqe)
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

func setupBench(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
}
