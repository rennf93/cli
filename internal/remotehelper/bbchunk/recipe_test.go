package bbchunk

import (
	"bytes"
	"errors"
	"testing"

	"github.com/jmwoliver/xet-go/xethash"
	"github.com/stretchr/testify/require"
)

// chunkBlob cuts data into chunks and packs them into xorbs in order. A xorb
// seals when the next chunk does not fit and at the end of the data. The
// recipe has one term per xorb, and names a repeated xorb once. This is the
// "xorbs of the input" recipe of the client vectors.
func chunkBlob(t *testing.T, data []byte) (*Recipe, []*Xorb) {
	t.Helper()
	r := &Recipe{Size: int64(len(data))}
	var xorbs []*Xorb
	var nodes []xethash.Node
	var b Builder
	seal := func() {
		x, err := b.Seal()
		require.NoError(t, err)
		i := len(r.Xorbs)
		for j, h := range r.Xorbs {
			if h == x.Hash {
				i = j
			}
		}
		if i == len(r.Xorbs) {
			r.Xorbs = append(r.Xorbs, x.Hash)
			xorbs = append(xorbs, x)
		}
		var unpacked uint64
		for _, c := range x.Chunks {
			unpacked += uint64(c.UnpackedSize)
		}
		r.Terms = append(r.Terms, Term{Xorb: i, ChunkStart: 0, ChunkEnd: len(x.Chunks), UnpackedBytes: unpacked})
	}
	err := Split(t.Context(), bytes.NewReader(data), func(c Chunk) error {
		_, err := b.Add(c.Data)
		if errors.Is(err, ErrXorbFull) {
			seal()
			_, err = b.Add(c.Data)
		}
		nodes = append(nodes, xethash.Node{Hash: c.Hash, Size: uint64(len(c.Data))})
		return err
	})
	require.NoError(t, err)
	if b.Chunks() > 0 {
		seal()
	}
	r.FileHash = xethash.File(nodes)
	return r, xorbs
}

func xorbHashes(xorbs []*Xorb) []xethash.Hash {
	hs := make([]xethash.Hash, len(xorbs))
	for i, x := range xorbs {
		hs[i] = x.Hash
	}
	return hs
}
