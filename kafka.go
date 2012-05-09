package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"
)

var ErrCrcMismatch = errors.New("CRC-Mismatch")

type reqType uint16

const (
	REQ_PRODUCE reqType = iota
	REQ_FETCH
	REQ_MULTIFETCH
	REQ_MULTIPRODUCE
	REQ_OFFSETS
)

var OFF_OLDEST uint64 = (1<<64 - 3) // uint64(max) - 2
var OFF_NEWEST uint64 = (1<<64 - 2) // uint64(max) - 1

type reqHeader struct {
	Length   uint32
	Request  uint16
	TopicLen uint16
}

type reqFooter struct {
	Partition uint32
	Offset    uint64
	Size      uint32
}

type request struct {
	reqHeader
	reqFooter
	topic []byte
}

func makeRequest(rType reqType, topic string, partition uint32, offset uint64, num uint32) request {
	req := request{}
	req.Length = uint32(24 + len(topic))
	req.Request = uint16(rType)
	req.TopicLen = uint16(len(topic))
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
	Length uint32
	Magic  byte
	Crc    uint32
}

type Reader struct {
	conn net.Conn
	r    *bytes.Buffer

	topic     string
	partition uint32
	offset    uint64
}

func Open(addr, topic string, partition uint32) (*Reader, error) {
	r, err := OpenWithOffset(addr, topic, partition, 0)
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

func OpenWithOffset(addr, topic string, partition uint32, offset uint64) (*Reader, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Reader{conn: conn, topic: topic, partition: partition, offset: offset}, nil
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

	length := uint32(0)
	if err := binary.Read(r.conn, binary.BigEndian, &length); err != nil {
		return err
	}

	r.r.Reset()
	_, err := io.CopyN(r.r, r.conn, int64(length))

	return err
}

func (r *Reader) Seek(offset uint64) {
	r.offset = offset
	r.r.Reset()
}

func (r *Reader) Read(buf []byte) (int, error) {
	if r.r.Len() == 0 {
		if err := r.fill(); err != nil {
			return 0, err
		}
	}

	msg := message{}
	if err := binary.Read(r.r, binary.BigEndian, &msg); err != nil {
		return 0, err
	}

	if len(buf) < int(msg.Length-5) {
		return 9, io.ErrShortBuffer
	}

	n, err := r.r.Read(buf[:msg.Length-5])
	if err != nil {
		return 9 + n, err
	}

	if crc32.ChecksumIEEE(buf[:msg.Length-5]) != msg.Crc {
		return 9 + n, ErrCrcMismatch
	}

	r.offset += 4 + uint64(msg.Length)
	return 9 + n, nil
}

func (r Reader) GetOffset() uint64 {
	return r.offset
}

func (r *Reader) Offsets(base uint64, num uint32) ([]uint64, error) {
	req := makeRequest(REQ_OFFSETS, r.topic, r.partition, base, num)
	if err := req.Write(r.conn); err != nil {
		return nil, err
	}

	length := uint32(0)
	if err := binary.Read(r.conn, binary.BigEndian, &length); err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	if _, err := io.CopyN(buf, r.conn, int64(length)); err != nil {
		return nil, err
	}

	numOffsets := uint32(0)
	if err := binary.Read(buf, binary.BigEndian, &numOffsets); err != nil {
		return nil, err
	}

	offsets := make([]uint64, numOffsets)
	for i := uint32(0); i < numOffsets; i++ {
		offset := uint64(0)
		if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
			return nil, err
		}

		offsets[i] = offset
	}

	return offsets, nil
}
