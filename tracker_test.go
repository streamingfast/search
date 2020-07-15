package search

import (
	"context"
	"testing"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dmesh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTrackerSort(t *testing.T) {
	newPeer := func(irrBlock uint64) *dmesh.SearchPeer {
		return &dmesh.SearchPeer{
			GenericPeer:    dmesh.GenericPeer{Ready: true},
			HasMovingHead:  true,
			BlockRangeData: dmesh.BlockRangeData{HeadBlockData: dmesh.HeadBlockData{IrrBlock: irrBlock}},
		}
	}

	tests := []struct {
		name        string
		peers       []*dmesh.SearchPeer
		threshold   int
		expectNum   uint64
		expectError error
	}{
		{
			name: "single",
			peers: []*dmesh.SearchPeer{
				newPeer(4),
			},
			threshold: 1,
			expectNum: 4,
		},
		{
			name: "in order",
			peers: []*dmesh.SearchPeer{
				newPeer(4),
				newPeer(6),
			},
			threshold: 1,
			expectNum: 6,
		},
		{
			name: "out of order",
			peers: []*dmesh.SearchPeer{
				newPeer(6),
				newPeer(4),
			},
			threshold: 1,
			expectNum: 6,
		},
		{
			name: "threshold = 2",
			peers: []*dmesh.SearchPeer{
				newPeer(6),
				newPeer(4),
			},
			threshold: 2,
			expectNum: 4,
		},
		// PANICS as expected:
		// {
		// 	name: "panics",
		// 	peers: []*dmesh.SearchPeer{
		// 		newPeer(6),
		// 		newPeer(4),
		// 	},
		// 	threshold:   0,
		// 	expectError: bstream.ErrTrackerBlockNotFound,
		// },
		{
			name:        "invalid threshold",
			peers:       []*dmesh.SearchPeer{},
			threshold:   1,
			expectError: bstream.ErrTrackerBlockNotFound,
		},
		{
			name: "has no moving head",
			peers: []*dmesh.SearchPeer{
				&dmesh.SearchPeer{HasMovingHead: false},
			},
			threshold:   1,
			expectError: bstream.ErrTrackerBlockNotFound,
		},
		{
			name: "is live",
			peers: []*dmesh.SearchPeer{
				&dmesh.SearchPeer{ServesReversible: true},
			},
			threshold:   1,
			expectError: bstream.ErrTrackerBlockNotFound,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := DmeshHighestArchiveBlockRefGetter(func() []*dmesh.SearchPeer { return test.peers }, test.threshold)
			res, err := f(context.Background())
			if test.expectError != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectNum, res.Num())
			}
		})
	}
}
