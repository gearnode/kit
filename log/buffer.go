package log

import (
	"bytes"
	"sync"
)

var bufPool = sync.Pool{
	New: func() any {
		return &bytes.Buffer{}
	},
}

func getBuffer() *bytes.Buffer {
	return bufPool.Get().(*bytes.Buffer)
}

func freeBuffer(bf *bytes.Buffer) {
	if bf.Cap() > 64*1024 {
		return
	}
	bf.Reset()
	bufPool.Put(bf)
}
