package kafka

import (
	"bytes"
	"testing"
)

func TestMessageRead(t *testing.T) {
	msg := []byte{0x00, 0x00, 0x00, 0x0c, 0x00, 0xe8, 0xf3, 0x5a, 0x06, 0x74, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67}

	r := Reader{r: bytes.NewBuffer(msg)}

	result := make([]byte, 7)
	if _, err := r.Read(result); err != nil {
		t.Error(err)
	}

	if string(result) != "testing" {
		t.Errorf("Expected 'testing', got '%s'", string(result))
	}
}
