package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"
)

var ErrCrcMismatch = errors.New("CRC-Mismatch")

type reqType int16

const (
	REQ_PRODUCE reqType = iota
	REQ_FETCH
	REQ_MULTIFETCH
	REQ_MULTIPRODUCE
	REQ_OFFSETS
)

var OFF_OLDEST int64 = -2
var OFF_NEWEST int64 = -1

type reqHeader struct {
	Length   int32
	Request  int16
	TopicLen int16
}

type reqFooter struct {
	Partition int32
	Offset    int64
	Size      int32
}

type request struct {
	reqHeader
	reqFooter
	topic []byte
}

func makeRequest(rType reqType, topic string, partition int32, offset int64, num int32) request {
	req := request{}
	req.Length = int32(20 + len(topic))
	req.Request = int16(rType)
	req.TopicLen = int16(len(topic))
	req.topic = []byte(topic)
	req.Partition = partition
	req.Offset = offset
	req.Size = num

	return req
}

func (r request) Write(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, &(r.reqHeader)); err != nil {
		return err
	}

	if _, err := w.Write(r.topic); err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, &(r.reqFooter))
}

type message struct {
	Length int32
	Magic  byte
	Crc    int32
}

type response struct {
	Length    int32
	ErrorCode int16
}

type ReaderOptions struct {
	ReadBuffer *bytes.Buffer
	RetryDelay time.Duration
}

func DefaultReaderOptions() ReaderOptions {
	return ReaderOptions{bytes.NewBuffer(make([]byte, 1024*1024)), 100 * time.Millisecond}
}

type Reader struct {
	conn net.Conn
	ro   ReaderOptions

	topic     string
	partition int32
	offset    int64
}

func Open(addr, topic string, partition int32, options ReaderOptions) (*Reader, error) {
	r, err := OpenWithOffset(addr, topic, partition, 0, options)
	if err != nil {
		r.Close()
		return nil, err
	}

	offsets, err := r.Offsets(OFF_NEWEST, 1)
	if err != nil {
		r.Close()
		return nil, err
	}

	r.Seek(offsets[0])
	return r, nil
}

func OpenWithOffset(addr, topic string, partition int32, offset int64, options ReaderOptions) (*Reader, error) {
	conn, err := net.DialTimeout("tcp", addr, time.Second*2)
	if err != nil {
		return nil, err
	}

	return &Reader{conn: conn, topic: topic, partition: partition, offset: offset, ro: options}, nil
}

func (r *Reader) Close() error {
	return r.conn.Close()
}

const MAX_BUFFER = 1024 * 1024

func (r *Reader) fill() error {
	req := makeRequest(REQ_FETCH, r.topic, r.partition, r.offset, MAX_BUFFER)
	if err := req.Write(r.conn); err != nil {
		return err
	}

	resp := response{}
	if err := binary.Read(r.conn, binary.BigEndian, &resp); err != nil {
		return err
	} else if resp.ErrorCode != 0 {
		r.ro.ReadBuffer.Reset()
		return errors.New(fmt.Sprintf("Kafka error: %d", resp.ErrorCode))
	}

	r.ro.ReadBuffer.Reset()
	_, err := io.CopyN(r.ro.ReadBuffer, r.conn, int64(resp.Length-2))

	return err
}

func (r *Reader) Seek(offset int64) {
	r.offset = offset
	r.ro.ReadBuffer.Reset()
}

func (r *Reader) Read(buf []byte) (int, error) {
	for r.ro.ReadBuffer.Len() == 0 {
		if err := r.fill(); err != nil {
			return 0, err
		}

		if r.ro.ReadBuffer.Len() > 0 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	msg := message{}
	if err := binary.Read(r.ro.ReadBuffer, binary.BigEndian, &msg); err != nil {
		return 0, err
	}

	if len(buf) < int(msg.Length-5) {
		return 9, io.ErrShortBuffer
	}

	n, err := r.ro.ReadBuffer.Read(buf[:msg.Length-5])
	if err != nil {
		return 9 + n, err
	}

	if crc32.ChecksumIEEE(buf[:msg.Length-5]) != uint32(msg.Crc) {
		return 9 + n, ErrCrcMismatch
	}

	r.offset += 4 + int64(msg.Length)
	return 9 + n, nil
}

func (r Reader) GetOffset() int64 {
	return r.offset
}

func (r *Reader) Offsets(base int64, num int32) ([]int64, error) {
	req := makeRequest(REQ_OFFSETS, r.topic, r.partition, base, num)
	if err := req.Write(r.conn); err != nil {
		return nil, err
	}

	resp := response{}
	if err := binary.Read(r.conn, binary.BigEndian, &resp); err != nil {
		return nil, err
	} else if resp.ErrorCode != 0 {
		return nil, errors.New(fmt.Sprintf("Kafka error: %d", resp.ErrorCode))
	}

	buf := new(bytes.Buffer)
	if _, err := io.CopyN(buf, r.conn, int64(resp.Length-2)); err != nil {
		return nil, err
	}

	numOffsets := int32(0)
	if err := binary.Read(buf, binary.BigEndian, &numOffsets); err != nil {
		return nil, err
	}

	offsets := make([]int64, numOffsets)
	for i := int32(0); i < numOffsets; i++ {
		offset := int64(0)
		if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
			return nil, err
		}

		offsets[i] = offset
	}

	return offsets, nil
}
