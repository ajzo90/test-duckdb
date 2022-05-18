package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"time"
)

type Field struct {
	Name string
	Type string
}

var model = map[string][]Field{
	"users": {
		{Name: "id", Type: "u32"},
		{Name: "age", Type: "u8"},
		{Name: "name", Type: "string"},
	},
	"transactions": {
		{Name: "id", Type: "u32"},
		{Name: "user", Type: "u32"},
		{Name: "item", Type: "u32"},
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

func handleStreamRequest(_w io.Writer, r *Req) error {

	fields := model[r.Table]
	if len(fields) == 0 {
		return fmt.Errorf("table not found")
	} else if r.Batch == 0 {
		return fmt.Errorf("batch can not be 0")
	}
	if r.Limit == 0 {
		r.Limit = 10000
	}

	var emitters []func(n int) []byte
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	staticRandomEmitter := func(size int) func(int) []byte {
		var buf = make([]byte, size*int(r.Batch))
		return func(n int) []byte {
			io.ReadFull(rnd, buf)
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

	w := bufio.NewWriter(_w)

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

	return w.Flush()
}
func main() {
	log.Fatalln(Main())
}