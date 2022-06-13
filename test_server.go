package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
)

type Field struct {
	Name string
	Type string
}

var model = map[string][]Field{
	"users": {
		{Name: "id", Type: "i32"},
		{Name: "age", Type: "i8"},
		{Name: "name", Type: "string"},
	},
	"transactions": {
		{Name: "id", Type: "i32"},
// 		{Name: "user", Type: "i32"},
// 		{Name: "item", Type: "i32"},
	},
}

type Req struct {
	Table  string
	Fields []string
	Batch  uint
	Limit  uint
}

func Main() error {
	return http.ListenAndServe(":6789", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/data-model":
			json.NewEncoder(writer).Encode(model)
		case "/data-stream":
			r := &Req{}
			if err := json.NewDecoder(request.Body).Decode(r); err != nil {
				http.Error(writer, err.Error(), 400)
				return
			} else if err := handleStreamRequest(writer, r); err != nil {
				http.Error(writer, err.Error(), 500)
				return
			}
		}
	}))
}

var cacheLock sync.Mutex
var cache = make(map[string][]byte)
var use_cache = true

func handleStreamRequest(_w io.Writer, r *Req) error {
	var rKeyBytes, _ = json.Marshal(r)
	var rKey = string(rKeyBytes)
	if use_cache {
		cacheLock.Lock()
		var cachedResponse, exists = cache[rKey]
		cacheLock.Unlock()
		if exists {
			_w.Write(cachedResponse)
			return nil
		}
	}

	fields := model[r.Table]
	if len(fields) == 0 {
		return fmt.Errorf("table not found")
	} else if r.Batch == 0 {
		return fmt.Errorf("batch can not be 0")
	}
	if r.Limit == 0 {
		r.Limit = 100_000_000
	}

	var emitters []func(n int) []byte
	rnd := rand.New(rand.NewSource(0))

	staticRandomEmitter := func(size int) func(int) []byte {
		var buf = make([]byte, size*int(r.Batch))
		var leading_zeros = make([]byte, 0*size/2)
		return func(n int) []byte {
			io.ReadFull(rnd, buf)
			for i := 0; i < int(r.Batch); i++ {
				copy(buf[i*size+size-len(leading_zeros):], leading_zeros)
			}
			return buf[:n*size]
		}
	}

	randomStringEmitter := func() func(n int) []byte {
		var buf = make([]byte, (2+16)*r.Batch)
		values := []string{"christian", "florian", "steve", "elon"}
		return func(n int) []byte {
			buf = buf[:2*n]
			for i := 0; i < n; i++ {
				v := values[rnd.Intn(len(values))]
				var sz = len(v)
				binary.LittleEndian.PutUint16(buf[2*i:], uint16(sz))
				buf = append(buf, v...)
			}
			return buf
		}
	}

	for _, v := range r.Fields {
		var emitter func(n int) []byte
		for _, f := range fields {
			if v != f.Name {
				continue
			}
			switch f.Type {
			case "string":
				emitter = randomStringEmitter()
			case "u64", "i64", "f64":
				emitter = staticRandomEmitter(8)
			case "u32", "i32", "f32":
				emitter = staticRandomEmitter(4)
			case "u16", "i16":
				emitter = staticRandomEmitter(2)
			case "u8", "i8":
				emitter = staticRandomEmitter(1)
			default:
				return fmt.Errorf("type %s not supported", f.Type)
			}
		}
		if emitter == nil {
			return fmt.Errorf("column not found")
		}
		emitters = append(emitters, emitter)
	}

	var batchSizeBuf [4]byte

	var w io.Writer
	if use_cache {
		w = bytes.NewBuffer(nil)
	} else {
		w = bufio.NewWriter(_w)
	}

	for capacity := r.Limit; capacity > 0; {
		toEmit := r.Batch
		if toEmit > capacity {
			toEmit = capacity
		}
		capacity -= toEmit
		binary.LittleEndian.PutUint32(batchSizeBuf[:], uint32(toEmit))
		if _, err := w.Write(batchSizeBuf[:]); err != nil {
			return err
		}
		for _, emitter := range emitters {
			if _, err := w.Write(emitter(int(toEmit))); err != nil {
				return err
			}
		}
	}

	binary.LittleEndian.PutUint32(batchSizeBuf[:], 0)

	if _, err := w.Write(batchSizeBuf[:]); err != nil {
		return err
	}

	if use_cache {
		cacheLock.Lock()
		defer cacheLock.Unlock()
		var cachedResponse = w.(*bytes.Buffer).Bytes()
		cache[rKey] = cachedResponse
		if _, err := _w.Write(cachedResponse); err != nil {
			return err
		}

	}
	return nil
}
func main() {
	log.Fatalln(Main())
}
