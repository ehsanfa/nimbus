package binary

import (
	"bytes"
	"context"
	"errors"
	"io"
)

func EncodeIdentifier(identifier byte, w io.Writer) error {
	buf := make([]byte, 1)
	buf[0] = identifier
	_, err := w.Write(buf)
	return err
	// return binary.Write(w, binary.BigEndian, identifier)
}

func DecodeIdentifier(r io.Reader) (byte, error) {
	buf := make([]byte, 1)
	r.Read(buf)
	return buf[0], nil
}

func EncodeBool(b bool, w io.Writer) error {
	buf := make([]byte, 1)
	if b {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	_, err := w.Write(buf)
	return err
	// return binary.Write(w, binary.BigEndian, b)
}

func DecodeBool(r io.Reader) (*bool, error) {
	buf := make([]byte, 1)
	r.Read(buf)
	b := buf[0] == 1
	return &b, nil
	// var b bool
	// if err := binary.Read(r, binary.BigEndian, &b); err != nil {
	// 	return nil, err
	// }
	// return &b, nil
}

func EncodeProposal(p uint64, w io.Writer) error {
	return EncodeUInt64(p, w)
	// return binary.Write(w, binary.BigEndian, p)
}

func DecodeProposal(r io.Reader) (*uint64, error) {
	// var n uint64
	n, err := DecodeUInt64(r)

	// if err := binary.Read(r, binary.BigEndian, &n); err != nil {
	// 	return nil, err
	// }
	return &n, err
}

func EncodeUInt64(n uint64, w io.Writer) error {
	buf := make([]byte, 8)
	buf[0] = byte(n >> 56)
	buf[1] = byte(n >> 48)
	buf[2] = byte(n >> 40)
	buf[3] = byte(n >> 32)
	buf[4] = byte(n >> 24)
	buf[5] = byte(n >> 16)
	buf[6] = byte(n >> 8)
	buf[7] = byte(n)
	_, err := w.Write(buf)
	return err
}

func DecodeUInt64(r io.Reader) (uint64, error) {
	buf := make([]byte, 8)
	_, err := r.Read(buf)
	return uint64(buf[0])<<56 |
		uint64(buf[1])<<48 |
		uint64(buf[2])<<40 |
		uint64(buf[3])<<32 |
		uint64(buf[4])<<24 |
		uint64(buf[5])<<16 |
		uint64(buf[6])<<8 |
		uint64(buf[7]), err
}

func EncodeUInt32(n uint32, w io.Writer) error {
	buf := make([]byte, 4)
	buf[0] = byte(n >> 24)
	buf[1] = byte(n >> 16)
	buf[2] = byte(n >> 8)
	buf[3] = byte(n)
	_, err := w.Write(buf)
	return err
}

func DecodeUInt32(r io.Reader) (uint32, error) {
	buf := make([]byte, 4)
	_, err := r.Read(buf)
	return uint32(buf[0])<<24 |
		uint32(buf[1])<<16 |
		uint32(buf[2])<<8 |
		uint32(buf[3]), err
}

func EncodeString(s string, w io.Writer) error {
	if err := EncodeUInt32(uint32(len(s)), w); err != nil {
		return err
	}
	// if err := binary.Write(w, binary.BigEndian, uint32(len(s))); err != nil {
	// 	return err
	// }
	if _, err := w.Write([]byte(s)); err != nil {
		return err
	}
	return nil
}

func EncodeBytes(s []byte, w io.Writer) error {
	if err := EncodeUInt32(uint32(len(s)), w); err != nil {
		return err
	}
	// if err := binary.Write(w, binary.BigEndian, uint32(len(s))); err != nil {
	// 	return err
	// }
	if _, err := w.Write(s); err != nil {
		return err
	}
	return nil
}

func DecodeString(r io.Reader) (*string, error) {
	l, err := DecodeUInt32(r)
	if err != nil {
		return nil, err
	}
	// var l uint32
	// if err := binary.Read(r, binary.BigEndian, &l); err != nil {
	// 	return nil, err
	// }
	if l == 0 {
		return nil, nil
	}
	bs := make([]byte, l)
	if _, err := r.Read(bs); err != nil {
		return nil, err
	}

	s := string(bs)

	return &s, nil
}

func DecodeStringToBytes(r io.Reader) ([]byte, error) {
	l, err := DecodeUInt32(r)
	if err != nil {
		return nil, err
	}
	if l == 0 {
		return make([]byte, 0), nil
	}
	bs := make([]byte, l)
	if _, err := r.Read(bs); err != nil {
		return make([]byte, 0), err
	}

	return bs, nil
}

func ContextfulWrite(ctx context.Context, w io.Writer, b bytes.Buffer) error {
	res := make(chan error)
	var bl bytes.Buffer
	if err := EncodeUInt32(uint32(b.Len()), &bl); err != nil {
		return err
	}
	// binary.Write(&bl, binary.BigEndian, uint32(b.Len()))
	b.WriteTo(&bl)
	go func() {
		_, err := bl.WriteTo(w)
		res <- err
	}()
	select {
	case err := <-res:
		return err
	case <-ctx.Done():
		return errors.New("context closed")
	}
}
