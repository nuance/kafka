# github.com/nuance/kafka

## Example

A simple, go-like Go Kafka consumer. Use it like follows:

	package main

    import (
    	"fmt"
    	"github.com/nuance/kafka"
   	)

   	func main() {
   		r, _ := kafka.Open("localhost:1234", "topic", 0)

   		buf := make([]byte, 1024)
   		for _, err := r.Read(buf); err == nil; _, err = r.Read(buf) {
   			fmt.Printf("'%s'", string(buf))
	   	}	
    }

The kafka module exposes a very simple reader interface:

    type Reader interface {
    	Seek(offset uint64)
    	Read(buf []byte) (int, error)
    	GetOffset() uint64
    	Close() error

    	Offsets(base uint64, num uint32) ([]uint64, error)
    }

Read will read exactly one message or fail.

## Warning

This API is subject to change. I'd like to eventually add support for publishers, and probably move the Offsets call off of the reader interface.
