package bbchunk

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/jmwoliver/xet-go/xethash"
	"github.com/jmwoliver/xet-go/xorb"
	"github.com/stretchr/testify/require"
)

// builderChunks returns chunks that select every compression scheme: random
// bytes stay uncompressed, a repeated byte takes LZ4 and a ramp of 32-bit
// words takes the byte grouping.
func builderChunks() [][]byte {
	random := randomBytes(40<<10, 1)
	ramp := make([]byte, 64<<10)
	for i := range ramp {
		ramp[i] = byte(i / 4 * (i % 4))
	}
	return [][]byte{random, bytes.Repeat([]byte{'a'}, 16<<10), ramp, []byte("tail")}
}

func randomBytes(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, 2))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func build(t *testing.T, chunks [][]byte) *Xorb {
	t.Helper()
	var b Builder
	for i, c := range chunks {
		h, err := b.Add(c)
		require.NoError(t, err, "chunk %d", i)
		require.Equal(t, xethash.Chunk(c), h, "chunk %d", i)
	}
	x, err := b.Seal()
	require.NoError(t, err)
	require.Zero(t, b.Chunks(), "Seal empties the builder")
	return x
}

// TestBuilderMatchesEncode holds the builder to the bytes that xet-go writes
// for the same chunks. entiredb's xorbstore.Builder matches the same bytes,
// so the server stores what it would have built itself.
func TestBuilderMatchesEncode(t *testing.T) {
	t.Parallel()
	chunks := builderChunks()
	x := build(t, chunks)

	obj, err := xorb.Encode(chunks, xorb.Auto)
	require.NoError(t, err)
	require.Equal(t, obj.Hash, x.Hash)
	require.Equal(t, obj.Body, x.Network)
	schemes := map[xorb.Scheme]bool{}
	require.Len(t, x.Chunks, len(obj.Chunks))
	for i, c := range obj.Chunks {
		require.Equal(t, ChunkInfo{Hash: c.Hash, UnpackedSize: c.UncompressedLength, StoredSize: c.SerializedLength}, x.Chunks[i])
		schemes[c.Scheme] = true
	}
	require.Len(t, schemes, 3, "the chunks must cover every scheme")
}

// TestBuilderIngestRoundTrip runs the check that the upload endpoint runs:
// xorb.IngestNetwork rehashes every chunk against the xorb hash.
func TestBuilderIngestRoundTrip(t *testing.T) {
	t.Parallel()
	chunks := builderChunks()
	x := build(t, chunks)

	in, err := xorb.IngestNetwork(x.Network, x.Hash)
	require.NoError(t, err)
	require.Equal(t, x.Hash, in.Hash)
	require.Len(t, in.Chunks, len(x.Chunks))
	for i, c := range in.Chunks {
		require.Equal(t, x.Chunks[i].Hash, c.Hash, "chunk %d", i)
		require.Equal(t, x.Chunks[i].UnpackedSize, c.UncompressedLength, "chunk %d", i)
		require.Equal(t, x.Chunks[i].StoredSize, c.SerializedLength, "chunk %d", i)
	}

	obj, err := xorb.Encode(chunks, xorb.Auto)
	require.NoError(t, err)
	complete, err := xorb.EncodeComplete(obj, [4]byte{})
	require.NoError(t, err)
	require.Equal(t, complete, in.Complete)

	dec, err := xorb.Decode(x.Network)
	require.NoError(t, err)
	for i, c := range dec.Chunks {
		require.Equal(t, chunks[i], c.Data, "chunk %d", i)
	}

	_, err = xorb.IngestNetwork(x.Network, xethash.Chunk([]byte("other")))
	require.Error(t, err)
}

func TestBuilderChunkLimit(t *testing.T) {
	t.Parallel()
	var b Builder
	for i := range xorb.MaxChunks {
		_, err := b.Add([]byte{byte(i), byte(i >> 8)})
		require.NoError(t, err)
	}
	_, err := b.Add([]byte{0})
	require.ErrorIs(t, err, ErrXorbFull)
	require.Equal(t, xorb.MaxChunks, b.Chunks())

	x, err := b.Seal()
	require.NoError(t, err)
	require.Len(t, x.Chunks, xorb.MaxChunks)
	_, err = xorb.IngestNetwork(x.Network, x.Hash)
	require.NoError(t, err)
}

func TestBuilderRawSizeLimit(t *testing.T) {
	t.Parallel()
	full := randomBytes(maxChunkSize, 3)
	var b Builder
	for i := range xorb.MaxRawSize/maxChunkSize - 1 {
		full[0] = byte(i)
		_, err := b.Add(full)
		require.NoError(t, err)
	}
	_, err := b.Add(full[:maxChunkSize-1])
	require.NoError(t, err)
	_, err = b.Add([]byte("ab"))
	require.ErrorIs(t, err, ErrXorbFull, "one byte past the limit")
	_, err = b.Add([]byte("a"))
	require.NoError(t, err, "exactly at the limit")
	_, err = b.Add([]byte("b"))
	require.ErrorIs(t, err, ErrXorbFull)

	x, err := b.Seal()
	require.NoError(t, err)
	var unpacked int
	for _, c := range x.Chunks {
		unpacked += int(c.UnpackedSize)
	}
	require.Equal(t, xorb.MaxRawSize, unpacked)
	require.LessOrEqual(t, len(x.Network), xorb.MaxNetworkSize)
	_, err = xorb.IngestNetwork(x.Network, x.Hash)
	require.NoError(t, err)
}

func TestBuilderSealEmpty(t *testing.T) {
	t.Parallel()
	var b Builder
	_, err := b.Seal()
	require.Error(t, err)
}

func TestBuilderRejectsBadChunk(t *testing.T) {
	t.Parallel()
	var b Builder
	_, err := b.Add(nil)
	require.Error(t, err)
	_, err = b.Add(make([]byte, maxChunkSize+1))
	require.Error(t, err)
	require.Zero(t, b.Chunks())
}
