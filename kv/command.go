// Package kv implements a linearizable, replicated key-value store on top of
// the raft package.
//
// Every operation, including reads, is committed through the Raft log before
// it executes, so all replicas apply the same operations in the same order
// and every response reflects a single global order consistent with real
// time. Requests may carry a client ID and sequence number; the state machine
// remembers the latest result per client, so a retried request after a
// timeout or leader change executes exactly once.
package kv

import (
	"fmt"

	"github.com/kunwar-dev/quorumkv/internal/codec"
)

// Op is the kind of operation in a Command.
type Op uint8

const (
	OpGet Op = iota + 1
	OpPut
	OpDelete
)

func (o Op) String() string {
	switch o {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	default:
		return fmt.Sprintf("op(%d)", uint8(o))
	}
}

// Command is a single client request, replicated through the log.
type Command struct {
	Op    Op
	Key   string
	Value []byte
	// ClientID and Seq enable exactly-once execution across retries. Seq must
	// increase for each new request from the same client. An empty ClientID
	// disables deduplication.
	ClientID string
	Seq      uint64
}

// Result is the outcome of applying a Command.
type Result struct {
	Value []byte
	// Found reports whether the key existed (Get, Delete).
	Found bool
}

const commandVersion = 1

// Encode serialises the command for the Raft log.
func (c Command) Encode() []byte {
	e := codec.Encoder{Buf: make([]byte, 0, 16+len(c.Key)+len(c.Value)+len(c.ClientID))}
	e.Byte(commandVersion)
	e.Byte(byte(c.Op))
	e.String(c.Key)
	e.Bytes(c.Value)
	e.String(c.ClientID)
	e.Uvarint(c.Seq)
	return e.Buf
}

// DecodeCommand parses a command produced by Encode.
func DecodeCommand(b []byte) (Command, error) {
	d := codec.NewDecoder(b)
	if v := d.Byte(); d.Err() == nil && v != commandVersion {
		return Command{}, fmt.Errorf("kv: unsupported command version %d", v)
	}
	c := Command{
		Op:       Op(d.Byte()),
		Key:      d.String(),
		Value:    d.Bytes(),
		ClientID: d.String(),
		Seq:      d.Uvarint(),
	}
	if err := d.Err(); err != nil {
		return Command{}, fmt.Errorf("kv: decode command: %w", err)
	}
	return c, nil
}
