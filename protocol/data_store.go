package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

type DataStoreMessage struct {
	Cmd  byte
	Args []string
}

func (d *DataStoreMessage) Encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, d.Cmd); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, uint8(len(d.Args))); err != nil {
		fmt.Println(err)
		return err
	}

	for _, arg := range d.Args {
		if err := binary.Write(w, binary.BigEndian, uint32(len(arg))); err != nil {
			fmt.Println(err)
			return err
		}
		if _, err := w.Write([]byte(arg)); err != nil {
			fmt.Println(err)
			return err
		}
	}

	return nil
}

func Decode(r io.Reader) (*DataStoreMessage, error) {
	m := &DataStoreMessage{}

	if err := binary.Read(r, binary.BigEndian, &m.Cmd); err != nil {
		return nil, err
	}

	var argsCount uint8
	if err := binary.Read(r, binary.BigEndian, &argsCount); err != nil {
		return nil, err
	}

	args := []string{}
	for range argsCount {
		var len uint32
		if err := binary.Read(r, binary.BigEndian, &len); err != nil {
			return nil, err
		}

		arg := make([]byte, len)
		if _, err := io.ReadFull(r, arg); err != nil {
			return nil, err
		}

		args = append(args, string(arg))
	}

	m.Args = args
	return m, nil
}
