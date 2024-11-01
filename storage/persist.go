package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"time"
)

const SET_COMMAND rune = 's'
const DELETE_COMMAND rune = 'd'

type entryLog struct {
	command rune
	key     string
	value   []byte
}

type writeAheadLog struct {
	file    *os.File
	entries chan entryLog
	buffer  bytes.Buffer
}

func (w *writeAheadLog) append(e entryLog) error {
	w.entries <- e
	return nil
}

func (w *writeAheadLog) read(logs chan entryLog) {
	defer close(logs)
	path := fmt.Sprintf("%s/wal.nimbus", "/tmp/nimbus/data")
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		panic(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 1024*1024)
	for sc.Scan() {
		k := []byte{}
		v := []byte{}
		divided := false
		for _, c := range sc.Text() {
			if c == ' ' {
				divided = true
				continue
			}
			if !divided {
				k = append(k, byte(c))
			} else {
				v = append(v, byte(c))
			}
		}
		key, err := base64.StdEncoding.DecodeString(string(k))
		if err != nil {
			fmt.Println("decode error", err, string(k))
		}
		val, err := base64.StdEncoding.DecodeString(string(v))
		if err != nil {
			fmt.Println("decode error for val", err, string(v))
		}
		logs <- entryLog{command: 's', key: string(key), value: []byte(val)}
	}
	if err := sc.Err(); err != nil {
		panic(err)
	}
}

func (w *writeAheadLog) write(e entryLog) {
	w.buffer.Write([]byte("\n"))
	w.buffer.Write([]byte(string([]rune{e.command})))
	w.buffer.Write([]byte(" "))
	w.buffer.Write([]byte(base64.StdEncoding.EncodeToString([]byte(e.key))))
	w.buffer.Write([]byte(" "))
	w.buffer.Write([]byte(base64.StdEncoding.EncodeToString([]byte(e.value))))
}

func (w *writeAheadLog) flush() error {
	_, err := w.file.Write(w.buffer.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (w *writeAheadLog) writeAndFlush(e entryLog) error {
	w.write(e)
	return w.flush()
}

func newWriteAheadLog(ctx context.Context, dir string, interval time.Duration) *writeAheadLog {
	path := fmt.Sprintf("%s/wal.nimbus", dir)
	err := os.Mkdir(dir, 0755)
	if err != nil {
		if !os.IsExist(err) {
			panic(err)
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if os.IsNotExist(err) {
		c, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		c.Close()
	}
	if err != nil {
		panic(err)
	}

	entries := make(chan entryLog)
	wal := &writeAheadLog{file: f, entries: entries}

	if interval > 0 {
		t := time.NewTicker(interval)
		go func() {
			for range t.C {
				err := wal.flush()
				if err != nil {
					fmt.Println(err)
				}
			}
		}()
	}

	go func() {
		for {
			select {
			case entry := <-entries:
				if interval == 0 {
					err := wal.writeAndFlush(entry)
					if err != nil {
						fmt.Println(err)
					}
				} else {
					wal.write(entry)
				}
			case <-ctx.Done():
				f.Close()
				fmt.Println("file closed")
				return
			}
		}
	}()

	return wal
}
