package fasthttpadaptor

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

var (
	defaultClientsCount = runtime.NumCPU()
	fakeResponse        = make([]byte, 1<<20)
	getRequest          = "GET /foobar?baz HTTP/1.1\r\nHost: google.com\r\nUser-Agent: aaa/bbb/ccc/ddd/eee Firefox Chrome MSIE Opera\r\n" +
		"Referer: http://example.com/aaa?bbb=ccc\r\nCookie: foo=bar; baz=baraz; aa=aakslsdweriwereowriewroire\r\n\r\n"
	postRequest = fmt.Sprintf("POST /foobar?baz HTTP/1.1\r\nHost: google.com\r\nContent-Type: foo/bar\r\nContent-Length: %d\r\n"+
		"User-Agent: Opera Chrome MSIE Firefox and other/1.2.34\r\nReferer: http://google.com/aaaa/bbb/ccc\r\n"+
		"Cookie: foo=bar; baz=baraz; aa=aakslsdweriwereowriewroire\r\n\r\n%q",
		len(fakeResponse), fakeResponse)
)

func BenchmarkAdaptorGet1ReqPerConn(b *testing.B) {
	benchmarkAdaptorGet(b, defaultClientsCount, 1)
}

func BenchmarkAdaptorGet2ReqPerConn(b *testing.B) {
	benchmarkAdaptorGet(b, defaultClientsCount, 2)
}

func BenchmarkAdaptorGet10ReqPerConn(b *testing.B) {
	benchmarkAdaptorGet(b, defaultClientsCount, 10)
}

func BenchmarkAdaptorGet10KReqPerConn(b *testing.B) {
	benchmarkAdaptorGet(b, defaultClientsCount, 10000)
}
func BenchmarkAdaptorGet1ReqPerConn10KClients(b *testing.B) {
	benchmarkAdaptorGet(b, 10000, 1)
}

func BenchmarkAdaptorGet2ReqPerConn10KClients(b *testing.B) {
	benchmarkAdaptorGet(b, 10000, 2)
}

func BenchmarkAdaptorGet10ReqPerConn10KClients(b *testing.B) {
	benchmarkAdaptorGet(b, 10000, 10)
}

func BenchmarkAdaptorGet100ReqPerConn10KClients(b *testing.B) {
	benchmarkAdaptorGet(b, 10000, 100)
}

func BenchmarkAdaptorPost1ReqPerConn(b *testing.B) {
	benchmarkAdaptorPost(b, defaultClientsCount, 1)
}

func BenchmarkAdaptorPost2ReqPerConn(b *testing.B) {
	benchmarkAdaptorPost(b, defaultClientsCount, 2)
}

func BenchmarkAdaptorPost10ReqPerConn(b *testing.B) {
	benchmarkAdaptorPost(b, defaultClientsCount, 10)
}

func BenchmarkAdaptorPost10KReqPerConn(b *testing.B) {
	benchmarkAdaptorPost(b, defaultClientsCount, 10000)
}

func benchmarkAdaptorGet(b *testing.B, clientsCount, requestsPerConn int) {
	ch := make(chan struct{}, b.N)
	s := &fasthttp.Server{
		Handler: NewFastHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.Method != fasthttp.MethodGet {
				b.Fatalf("Unexpected request method: %q", req.Method)
			}
			h := w.Header()
			h.Set("Content-Type", "text/plain")
			if requestsPerConn == 1 {
				h.Set(fasthttp.HeaderConnection, "close")
			}
			w.Write(fakeResponse) //nolint:errcheck
			registerServedRequest(b, ch)
		})),
		Concurrency: 16 * clientsCount,
	}
	benchmarkServer(b, s, clientsCount, requestsPerConn, getRequest)
	verifyRequestsServed(b, ch)
}

func benchmarkAdaptorPost(b *testing.B, clientsCount, requestsPerConn int) {
	ch := make(chan struct{}, b.N)
	s := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			if !ctx.IsPost() {
				b.Fatalf("Unexpected request method: %q", ctx.Method())
			}
			body := ctx.Request.Body()
			if !bytes.Equal(body, fakeResponse) {
				b.Fatalf("Unexpected body %q. Expected %q", body, fakeResponse)
			}
			ctx.Success("text/plain", body)
			if requestsPerConn == 1 {
				ctx.SetConnectionClose()
			}
			registerServedRequest(b, ch)
		},
		Concurrency: 16 * clientsCount,
	}
	benchmarkServer(b, s, clientsCount, requestsPerConn, postRequest)
	verifyRequestsServed(b, ch)
}

type fakeServerConn struct {
	net.TCPConn
	ln            *fakeListener
	requestsCount int
	pos           int
	closed        uint32
}

func (c *fakeServerConn) Read(b []byte) (int, error) {
	nn := 0
	reqLen := len(c.ln.request)
	for len(b) > 0 {
		if c.requestsCount == 0 {
			if nn == 0 {
				return 0, io.EOF
			}
			return nn, nil
		}
		pos := c.pos % reqLen
		n := copy(b, c.ln.request[pos:])
		b = b[n:]
		nn += n
		c.pos += n
		if n+pos == reqLen {
			c.requestsCount--
		}
	}
	return nn, nil
}

func (c *fakeServerConn) Write(b []byte) (int, error) {
	return len(b), nil
}

var fakeAddr = net.TCPAddr{
	IP:   []byte{1, 2, 3, 4},
	Port: 12345,
}

func (c *fakeServerConn) RemoteAddr() net.Addr {
	return &fakeAddr
}

func (c *fakeServerConn) Close() error {
	if atomic.AddUint32(&c.closed, 1) == 1 {
		c.ln.ch <- c
	}
	return nil
}

func (c *fakeServerConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (c *fakeServerConn) SetWriteDeadline(t time.Time) error {
	return nil
}

type fakeListener struct {
	lock            sync.Mutex
	requestsCount   int
	requestsPerConn int
	request         []byte
	ch              chan *fakeServerConn
	done            chan struct{}
	closed          bool
}

func (ln *fakeListener) Accept() (net.Conn, error) {
	ln.lock.Lock()
	if ln.requestsCount == 0 {
		ln.lock.Unlock()
		for len(ln.ch) < cap(ln.ch) {
			time.Sleep(10 * time.Millisecond)
		}
		ln.lock.Lock()
		if !ln.closed {
			close(ln.done)
			ln.closed = true
		}
		ln.lock.Unlock()
		return nil, io.EOF
	}
	requestsCount := ln.requestsPerConn
	if requestsCount > ln.requestsCount {
		requestsCount = ln.requestsCount
	}
	ln.requestsCount -= requestsCount
	ln.lock.Unlock()

	c := <-ln.ch
	c.requestsCount = requestsCount
	c.closed = 0
	c.pos = 0

	return c, nil
}

func (ln *fakeListener) Close() error {
	return nil
}

func (ln *fakeListener) Addr() net.Addr {
	return &fakeAddr
}

func newFakeListener(requestsCount, clientsCount, requestsPerConn int, request string) *fakeListener {
	ln := &fakeListener{
		requestsCount:   requestsCount,
		requestsPerConn: requestsPerConn,
		request:         []byte(request),
		ch:              make(chan *fakeServerConn, clientsCount),
		done:            make(chan struct{}),
	}
	for i := 0; i < clientsCount; i++ {
		ln.ch <- &fakeServerConn{
			ln: ln,
		}
	}
	return ln
}

func registerServedRequest(b *testing.B, ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
		b.Fatalf("More than %d requests served", cap(ch))
	}
}

func verifyRequestsServed(b *testing.B, ch <-chan struct{}) {
	requestsServed := 0
	for len(ch) > 0 {
		<-ch
		requestsServed++
	}
	requestsSent := b.N
	for requestsServed < requestsSent {
		select {
		case <-ch:
			requestsServed++
		case <-time.After(100 * time.Millisecond):
			b.Fatalf("Unexpected number of requests served %d. Expected %d", requestsServed, requestsSent)
		}
	}
}

type realServer interface {
	Serve(ln net.Listener) error
}

func benchmarkServer(b *testing.B, s realServer, clientsCount, requestsPerConn int, request string) {
	ln := newFakeListener(b.N, clientsCount, requestsPerConn, request)
	ch := make(chan struct{})
	go func() {
		s.Serve(ln) //nolint:errcheck
		ch <- struct{}{}
	}()

	<-ln.done

	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		b.Fatalf("Server.Serve() didn't stop")
	}
}
