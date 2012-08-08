package kafka

import (
	"bytes"
	"testing"
)

func TestMessageRead(t *testing.T) {
	msg := []byte{0x00, 0x00, 0x00, 0x0c, // length
		0x00,                   // magic
		0xe8, 0xf3, 0x5a, 0x06, // crc
		0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, // "testing"
	}

	options := DefaultConsumerOptions()
	options.ReadBuffer = bytes.NewBuffer(msg)
	r := Consumer{co: options}

	result := make([]byte, 7)
	if _, err := r.Read(result); err != nil {
		t.Error(err)
	}

	if string(result) != "testing" {
		t.Errorf("Expected 'testing', got '%s'", string(result))
	}
}

func TestOffsetsRequest(t *testing.T) {
	expected := []byte{
		0x00, 0x00, 0x00, 0x1B, // length
		0x00, 0x04, // request
		0x00, 0x07, // topic length
		0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, // "testing"
		0x00, 0x00, 0x00, 0x05, // partition
		0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // offset
		0x00, 0x00, 0x00, 0x02, // size
	}

	req := makeRequest(REQ_OFFSETS, "testing", 5, OFF_NEWEST, 2)

	buf := bytes.NewBuffer([]byte{})
	if err := req.Write(buf); err != nil {
		t.Error(err)
	}

	if string(buf.Bytes()) != string(expected) {
		for idx, char := range buf.Bytes() {
			if char != expected[idx] {
				t.Error("Byte at position", idx, "differs:", char, "vs. expected", expected[idx])
			}
		}

		t.Error("request isn't formatted correctly")
	}
}

func TestFetchRequest(t *testing.T) {
	expected := []byte{
		0x00, 0x00, 0x00, 0x1B, // length
		0x00, 0x01, // request
		0x00, 0x07, // topic length
		0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, // "testing"
		0x00, 0x00, 0x00, 0x05, // partition
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, // offset
		0x00, 0x00, 0x00, 0x10, // buffer size
	}

	req := makeRequest(REQ_FETCH, "testing", 5, 100, 16)

	buf := bytes.NewBuffer([]byte{})
	if err := req.Write(buf); err != nil {
		t.Error(err)
	}

	if string(buf.Bytes()) != string(expected) {
		t.Error("request isn't formatted correctly")
	}
}
