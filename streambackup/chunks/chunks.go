package chunks

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

var ErrNotFound = errors.New("not found")

type ChunkRange struct {
	L, R uint64
}

type Chunks struct {
	Dir                string
	ChunkNamePrefix    string
	CompressionLevel   int
	MaxEntriesPerChunk int
	MaxChunkSize       int

	chunks []ChunkRange

	writerTmpPath   string
	writerFile      *os.File
	writerGzip      *gzip.Writer
	writerTar       *tar.Writer
	writerChunkSize int

	readerFile     *os.File
	readerGzip     *gzip.Reader
	readerTar      *tar.Reader
	readerChunk    ChunkRange
	readerLastSeek uint64
}

func (c *Chunks) Open() error {
	if err := os.MkdirAll(c.Dir, 0755); err != nil {
		return err
	}

	r, err := regexp.Compile(fmt.Sprintf(`^%s(?P<First>\d{9})-(?P<Last>\d{9})\.tar\.gz$`, c.ChunkNamePrefix))
	if err != nil {
		return err
	}

	files, err := ioutil.ReadDir(c.Dir)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		match := r.FindStringSubmatch(file.Name())
		if match == nil {
			continue
		}
		if len(match) != 3 {
			continue
		}
		l, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			continue
		}
		r, err := strconv.ParseUint(match[2], 10, 64)
		if err != nil {
			continue
		}
		if l > r {
			continue
		}
		c.chunks = append(c.chunks, ChunkRange{L: l, R: r})
	}

	c.writerTmpPath = filepath.Join(c.Dir, fmt.Sprintf("%snext.tmp", c.ChunkNamePrefix))

	return nil
}

func (c *Chunks) GetLeftmostAbsentRange(min, max uint64) (l uint64, r uint64, err error) {
	if min > max {
		return 0, 0, fmt.Errorf("min > max")
	}

	l, r = min, max
	for _, chunk := range c.chunks {
		if chunk.R < l || chunk.L > r {
			continue
		}
		if chunk.L <= l && chunk.R >= r {
			return 0, 0, ErrNotFound
		}
		if chunk.L <= l {
			l = chunk.R + 1
		} else {
			r = chunk.L - 1
		}
	}
	return l, r, nil
}

func (c *Chunks) getChunkPath(chunk ChunkRange) string {
	return filepath.Join(c.Dir, fmt.Sprintf("%s%09d-%09d.tar.gz", c.ChunkNamePrefix, chunk.L, chunk.R))
}

func (c *Chunks) Flush() error {
	if c.writerFile == nil {
		return nil
	}
	if err := c.writerTar.Flush(); err != nil {
		return err
	}
	if err := c.writerTar.Close(); err != nil {
		return err
	}
	if err := c.writerGzip.Flush(); err != nil {
		return err
	}
	if err := c.writerGzip.Close(); err != nil {
		return err
	}
	if err := c.writerFile.Sync(); err != nil {
		return err
	}
	if err := c.writerFile.Close(); err != nil {
		return err
	}
	c.writerTar, c.writerGzip, c.writerFile = nil, nil, nil
	return os.Rename(c.writerTmpPath, c.getChunkPath(c.chunks[len(c.chunks)-1]))
}

func (c *Chunks) startNewChunk(pos uint64) error {
	if err := c.Flush(); err != nil {
		return err
	}
	var err error
	if c.writerFile, err = os.Create(c.writerTmpPath); err != nil {
		return err
	}
	if c.writerGzip, err = gzip.NewWriterLevel(c.writerFile, c.CompressionLevel); err != nil {
		c.writerFile.Close()
		c.writerFile = nil
		return err
	}
	c.writerTar = tar.NewWriter(c.writerGzip)
	c.chunks = append(c.chunks, ChunkRange{L: pos, R: pos})
	c.writerChunkSize = 0
	return nil
}

func (c *Chunks) Write(pos uint64, payload []byte) error {
	newChunkNeeded := false
	if c.writerFile == nil {
		newChunkNeeded = true
	} else {
		curChunk := c.chunks[len(c.chunks)-1]
		newChunkNeeded = curChunk.R != pos-1
		newChunkNeeded = newChunkNeeded || ((curChunk.R-curChunk.L+1)+1 > uint64(c.MaxEntriesPerChunk))
		newChunkNeeded = newChunkNeeded || (c.writerChunkSize+len(payload) > c.MaxChunkSize)
	}
	if newChunkNeeded {
		if err := c.startNewChunk(pos); err != nil {
			return err
		}
	}

	err := c.writerTar.WriteHeader(&tar.Header{
		Name: fmt.Sprintf("%09d", pos),
		Mode: 0666,
		Size: int64(len(payload)),
	})
	if err != nil {
		return err
	}

	numWritten, err := c.writerTar.Write(payload)
	if numWritten != len(payload) {
		return fmt.Errorf("numWritten [%d] != len(payload) [%d]", numWritten, len(payload))
	}
	if err != nil {
		return err
	}
	c.writerChunkSize += len(payload)
	c.chunks[len(c.chunks)-1].R = pos

	return nil
}

func (c *Chunks) CloseReader() error {
	if c.readerFile == nil {
		return nil
	}
	if err := c.readerFile.Close(); err != nil {
		return err
	}
	c.readerFile, c.readerGzip, c.readerTar = nil, nil, nil
	return nil
}

func (c *Chunks) SeekReader(pos uint64) error {
	if err := c.CloseReader(); err != nil {
		return err
	}

	idx := -1
	var leftmost uint64
	for i, chunk := range c.chunks {
		if chunk.R < pos {
			continue
		}
		candidate := pos
		if chunk.L > pos {
			candidate = chunk.L
		}
		if idx < 0 || candidate < leftmost {
			idx, leftmost = i, candidate
		}
	}
	if idx < 0 {
		return ErrNotFound
	}

	if c.writerFile != nil && idx == len(c.chunks)-1 {
		if err := c.Flush(); err != nil {
			return err
		}
	}

	c.readerChunk = c.chunks[idx]
	var err error
	c.readerFile, err = os.Open(c.getChunkPath(c.readerChunk))
	if err != nil {
		return err
	}
	c.readerGzip, err = gzip.NewReader(c.readerFile)
	if err != nil {
		c.readerFile.Close()
		c.readerFile = nil
		return err
	}
	c.readerTar = tar.NewReader(c.readerGzip)
	c.readerLastSeek = pos

	return nil
}

func (c *Chunks) ReadNext() (uint64, []byte, error) {
	if c.readerFile == nil {
		if err := c.SeekReader(0); err != nil {
			return 0, nil, err
		}
	}

	for {
		hdr, err := c.readerTar.Next()
		if err == io.EOF {
			if err := c.SeekReader(c.readerChunk.R + 1); err != nil {
				return 0, nil, err
			}
			continue
		}
		if err != nil {
			return 0, nil, err
		}
		cur, err := strconv.ParseUint(hdr.Name, 10, 64)
		if err != nil {
			return 0, nil, err
		}
		if cur < c.readerLastSeek {
			continue
		}
		data, err := io.ReadAll(c.readerTar)
		if err != nil {
			return 0, nil, err
		}
		if int64(len(data)) != hdr.Size {
			return 0, nil, fmt.Errorf("int64(len(data)) [%d] != hdr.Size [%d]", int64(len(data)), hdr.Size)
		}
		return cur, data, nil
	}
}

func (c *Chunks) GetChunkRanges() []ChunkRange {
	chunksCopy := make([]ChunkRange, len(c.chunks))
	copy(chunksCopy, c.chunks)
	return chunksCopy
}
