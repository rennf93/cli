package bbchunk

import (
	"errors"
	"fmt"

	"github.com/jmwoliver/xet-go/xethash"
	"github.com/jmwoliver/xet-go/xorb"
)

// ErrXorbFull reports a chunk that does not fit in the open xorb. Seal the
// xorb and add the chunk to the next one.
var ErrXorbFull = errors.New("bbchunk: xorb is full")

// ChunkInfo describes one chunk of a xorb.
type ChunkInfo struct {
	Hash xethash.Hash
	// UnpackedSize is the length of the chunk data.
	UnpackedSize uint32
	// StoredSize is the length of the chunk in the xorb: its 8-byte header
	// and its payload, compressed or not.
	StoredSize uint32
}

// Xorb is a sealed xorb.
type Xorb struct {
	Hash xethash.Hash
	// Network is the xorb in xet-go's network format: the chunks with no
	// footer. The upload endpoint takes these bytes. The server adds the
	// footer when it stores the xorb.
	Network []byte
	// Chunks lists the chunks in xorb order.
	Chunks []ChunkInfo
}

// Builder assembles one xorb in memory. It holds the stored chunks and the
// chunk table, and no copy of the unpacked data.
//
// Each chunk is compressed on its own with the scheme that xorb.Auto chooses,
// as the server does. So Network equals the body that xorb.Encode makes for
// the same chunks with xorb.Auto.
type Builder struct {
	body     []byte
	chunks   []ChunkInfo
	unpacked int
}

// Chunks returns the number of chunks in the builder.
func (b *Builder) Chunks() int { return len(b.chunks) }

// Add compresses data, one chunk, and appends it. It returns the chunk hash.
// It fails with ErrXorbFull when the xorb holds xorb.MaxChunks chunks or
// data would take the unpacked size past xorb.MaxRawSize.
func (b *Builder) Add(data []byte) (xethash.Hash, error) {
	if len(b.chunks) == xorb.MaxChunks || b.unpacked+len(data) > xorb.MaxRawSize {
		return xethash.Hash{}, ErrXorbFull
	}
	obj, err := xorb.Encode([][]byte{data}, xorb.Auto)
	if err != nil {
		return xethash.Hash{}, fmt.Errorf("bbchunk: encode chunk: %w", err)
	}
	c := obj.Chunks[0]
	b.body = append(grow(b.body, len(obj.Body)), obj.Body...)
	b.unpacked += len(data)
	b.chunks = append(b.chunks, ChunkInfo{Hash: c.Hash, UnpackedSize: c.UncompressedLength, StoredSize: c.SerializedLength})
	return c.Hash, nil
}

// Seal returns the xorb of the chunks added so far and empties the builder.
// The xorb owns its memory. Seal fails on an empty builder.
func (b *Builder) Seal() (*Xorb, error) {
	if len(b.chunks) == 0 {
		return nil, errors.New("bbchunk: seal an empty xorb")
	}
	nodes := make([]xethash.Node, len(b.chunks))
	for i, c := range b.chunks {
		nodes[i] = xethash.Node{Hash: c.Hash, Size: uint64(c.UnpackedSize)}
	}
	x := &Xorb{Hash: xethash.Xorb(nodes), Network: b.body, Chunks: b.chunks}
	*b = Builder{}
	return x, nil
}

// grow returns b with room for n more bytes. It doubles the capacity up to
// the largest network xorb, so a full xorb copies its bytes about once.
func grow(b []byte, n int) []byte {
	need := len(b) + n
	if need <= cap(b) {
		return b
	}
	nb := make([]byte, len(b), max(min(2*cap(b), xorb.MaxNetworkSize), need, 64<<10))
	copy(nb, b)
	return nb
}
