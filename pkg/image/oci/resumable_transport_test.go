package oci

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// flakyBlobServer serves a fixed payload, honouring Range requests, but aborts the connection
// partway through the body for the first dropsLeft attempts. this is how a CDN-fronted registry
// behaves on a multi-gigabyte layer.
type flakyBlobServer struct {
	mu           sync.Mutex
	payload      []byte
	dropsLeft    int
	bytesPerDrop int
	ranges       []string
	headers      []http.Header

	// rangeStatus, when non-zero, is served instead of 206 for a range request
	rangeStatus int

	// rangeStartSkew and totalSkew shift the start and total the server claims in Content-Range, as
	// a server resuming from the wrong place or serving a different object would
	rangeStartSkew int64
	totalSkew      int64

	// blankContentRange serves a 206 with an empty Content-Range, which the client cannot use
	blankContentRange bool

	// contentEncoding, when set, accompanies a range response
	contentEncoding string

	// abortAfterBody tears the connection down after the final byte rather than partway through
	abortAfterBody bool

	// restartOnRange echoes back the range that was asked for and then serves the whole object
	// from byte 0, as a server with no real range support does
	restartOnRange bool

	// transientStatus, when non-zero, is served for the first transientTimes range requests, after
	// which the server resumes correctly. transientTimes of 0 means for ever. this is a CDN edge
	// that is briefly unwell rather than one that cannot resume at all.
	transientStatus int
	transientTimes  int
	transientServed int

	// transientContentEncoding accompanies a transient refusal, as a CDN serving a pre-compressed
	// error page does whatever Accept-Encoding it was sent
	transientContentEncoding string
}

func (s *flakyBlobServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rangeHeader := r.Header.Get("Range")

	s.mu.Lock()
	s.ranges = append(s.ranges, rangeHeader)
	s.headers = append(s.headers, r.Header.Clone())
	drop := s.dropsLeft > 0
	if drop {
		s.dropsLeft--
	}

	transient := false
	if rangeHeader != "" && s.transientStatus != 0 &&
		(s.transientTimes == 0 || s.transientServed < s.transientTimes) {
		transient = true
		s.transientServed++
	}
	s.mu.Unlock()

	if transient {
		if s.transientContentEncoding != "" {
			w.Header().Set("Content-Encoding", s.transientContentEncoding)
		}
		w.WriteHeader(s.transientStatus)

		return
	}

	start, end := int64(0), int64(-1)
	status := http.StatusOK
	remaining := s.payload

	if rangeHeader != "" {
		start, end = parseRangeRequest(rangeHeader)
		status = http.StatusPartialContent
		if s.rangeStatus != 0 {
			status = s.rangeStatus
		}

		remaining = limitToRange(s.payload[start:], start, end)

		// what Content-Range will claim; normally the same as what is sent
		declared := int64(len(remaining))

		if s.restartOnRange {
			remaining = s.payload
		}

		if s.contentEncoding != "" {
			w.Header().Set("Content-Encoding", s.contentEncoding)
		}

		if s.blankContentRange {
			w.Header().Set("Content-Range", "")
		} else {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d",
				start+s.rangeStartSkew, start+declared-1, int64(len(s.payload))+s.totalSkew))
		}
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.Itoa(len(remaining)))
	w.WriteHeader(status)

	if drop {
		_, _ = w.Write(remaining[:s.bytesPerDrop])
		// force the partial write onto the wire before tearing the connection down, so the client
		// genuinely sees a truncated body rather than nothing at all
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}

	_, _ = w.Write(remaining)

	if s.abortAfterBody {
		// every declared byte has been delivered; only then is the connection torn down
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}
}

func (s *flakyBlobServer) seenRanges() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ranges...)
}

func (s *flakyBlobServer) seenHeaders() []http.Header {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]http.Header(nil), s.headers...)
}

// trickleServer delivers a single byte and then aborts, for ever. every attempt technically makes
// progress, which is what a budget replenished by any progress at all cannot survive.
type trickleServer struct {
	mu       sync.Mutex
	payload  []byte
	requests int
}

func (s *trickleServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests++
	s.mu.Unlock()

	start := int64(0)
	status := http.StatusOK
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		start, _ = parseRangeRequest(rangeHeader)
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(s.payload)-1, len(s.payload)))
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(s.payload)-int(start)))
	w.WriteHeader(status)

	_, _ = w.Write(s.payload[start : start+1])
	w.(http.Flusher).Flush()
	panic(http.ErrAbortHandler)
}

func (s *trickleServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

// parseRangeRequest returns the inclusive bounds of a closed range, or an end of -1 for an
// open-ended one. the fixtures honour the end so that they model what the transport actually sends.
func parseRangeRequest(header string) (int64, int64) {
	spec := strings.TrimPrefix(header, "bytes=")

	first, last, _ := strings.Cut(spec, "-")

	start, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		panic(err)
	}

	if last == "" {
		return start, -1
	}

	end, err := strconv.ParseInt(last, 10, 64)
	if err != nil {
		panic(err)
	}

	return start, end
}

// limitToRange trims a payload tail to the inclusive end of a requested range.
func limitToRange(tail []byte, start, end int64) []byte {
	if end < 0 {
		return tail
	}

	if want := end - start + 1; want >= 0 && want < int64(len(tail)) {
		return tail[:want]
	}

	return tail
}

func payloadOfSize(n int) []byte {
	payload := make([]byte, n)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	return payload
}

// newTestTransport builds a transport that resumes above the given size with no backoff, so the
// tests need neither large payloads nor real delays.
func newTestTransport(minSize int64) *resumableTransport {
	transport := newResumableTransport(http.DefaultTransport)
	transport.minSize = minSize
	transport.backoff = 0
	return transport
}

// stallingBlobServer sends a prefix, flushes it, and then goes quiet without closing the connection
// or writing another byte -- the shape an unhealthy CDN edge takes, and the one flakyBlobServer
// cannot produce, since that tears the connection down instead. every later attempt is served in
// full, so a test fails by hanging if the stalled read is never ended.
type stallingBlobServer struct {
	payload    []byte
	stallAfter int
	release    chan struct{}

	mu       sync.Mutex
	attempts int
	ranges   []string
}

func (s *stallingBlobServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.attempts++
	attempt := s.attempts
	s.ranges = append(s.ranges, r.Header.Get("Range"))
	s.mu.Unlock()

	start := int64(0)
	status := http.StatusOK

	if spec := r.Header.Get("Range"); spec != "" {
		var end int64
		if _, err := fmt.Sscanf(spec, "bytes=%d-%d", &start, &end); err != nil {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(s.payload)-1, len(s.payload)))
	}

	remaining := s.payload[start:]
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", strconv.Itoa(len(remaining)))
	w.WriteHeader(status)

	if attempt > 1 {
		_, _ = w.Write(remaining)
		return
	}

	// the first attempt delivers a prefix and then says nothing more, holding the connection open
	_, _ = w.Write(remaining[:s.stallAfter])
	w.(http.Flusher).Flush()
	<-s.release
}

func (s *stallingBlobServer) seenRanges() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ranges...)
}

// pacedBlobServer delivers the whole payload in small writes spaced apart, never pausing long
// enough to be a stall. it guards the constant: a deadline that tracked slowness rather than
// silence would abandon this transfer, which is exactly the reader the resume path exists for.
type pacedBlobServer struct {
	payload []byte
	writes  int
	gap     time.Duration

	mu       sync.Mutex
	attempts int
}

func (s *pacedBlobServer) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.attempts++
	s.mu.Unlock()

	w.Header().Set("Content-Length", strconv.Itoa(len(s.payload)))
	w.WriteHeader(http.StatusOK)

	size := len(s.payload) / s.writes
	for off := 0; off < len(s.payload); off += size {
		end := min(off+size, len(s.payload))

		_, _ = w.Write(s.payload[off:end])
		w.(http.Flusher).Flush()
		time.Sleep(s.gap)
	}
}

func (s *pacedBlobServer) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func newTestClient(minSize int64) *http.Client {
	return &http.Client{Transport: newTestTransport(minSize)}
}

func TestResumableTransport_reassemblesBlobAcrossDrops(t *testing.T) {
	payload := payloadOfSize(8192)
	server := &flakyBlobServer{payload: payload, dropsLeft: 3, bytesPerDrop: 1000}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "the reassembled body should be byte-identical to the original")

	// the first attempt carries no Range; each resume asks for exactly the bytes still owed, as a
	// closed range -- an open-ended one is answered with 416 by go-containerregistry's own registry
	assert.Equal(t, []string{"", "bytes=1000-8191", "bytes=2000-8191", "bytes=3000-8191"},
		server.seenRanges())
}

func TestResumableTransport_survivesManyDropsWhileMakingProgress(t *testing.T) {
	payload := payloadOfSize(2 * 1024 * 1024)

	// far more drops than the stall budget, each delivering well over minResumeProgress, so real
	// progress must keep the budget topped up or a long but healthy transfer would be abandoned
	server := &flakyBlobServer{payload: payload, dropsLeft: 14, bytesPerDrop: 128 * 1024}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
	assert.Greater(t, len(server.seenRanges()), maxConsecutiveStalls)
}

func TestResumableTransport_terminatesAgainstATricklingServer(t *testing.T) {
	// one byte per connection is progress by any naive measure, so a budget replenished by any
	// n > 0 would never be spent and the read would hang indefinitely
	server := &trickleServer{payload: payloadOfSize(1 << 20)}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	done := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(resp.Body)
		done <- readErr
	}()

	select {
	case readErr := <-done:
		require.Error(t, readErr)
		assert.Contains(t, readErr.Error(), "without progress")
		assert.LessOrEqual(t, server.count(), maxConsecutiveStalls+1)
	case <-time.After(10 * time.Second):
		t.Fatalf("read never terminated; %d requests issued", server.count())
	}
}

func TestResumableTransport_givesUpWhenNoProgressIsMade(t *testing.T) {
	// every attempt delivers zero bytes, so the stall budget is never replenished
	server := &flakyBlobServer{payload: payloadOfSize(8192), dropsLeft: 1000, bytesPerDrop: 0}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	_, err = io.ReadAll(resp.Body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "without progress")

	// the initial attempt plus exactly the stall budget, and no more
	assert.Len(t, server.seenRanges(), maxConsecutiveStalls+1)
}

func TestResumableTransport_refusesAResumeThatIsNotPartialContent(t *testing.T) {
	// a server answering a range request with the whole body would restart the stream, duplicating
	// everything already written
	server := &flakyBlobServer{
		payload:      payloadOfSize(8192),
		dropsLeft:    1000,
		bytesPerDrop: 512,
		rangeStatus:  http.StatusOK,
	}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	_, err = io.ReadAll(resp.Body)
	require.Error(t, err)
	assert.ErrorIs(t, err, errNoRangeSupport)
	assert.Contains(t, err.Error(), "after ", "the original drop should survive in the error chain")

	// a server that will not resume is refused at once rather than after the whole budget, so the
	// failure is no slower than it was before resuming existed
	assert.Len(t, server.seenRanges(), 2)
}

func TestResumableTransport_refusesAResumeFromTheWrongOffset(t *testing.T) {
	server := &flakyBlobServer{
		payload:        payloadOfSize(8192),
		dropsLeft:      1000,
		bytesPerDrop:   512,
		rangeStartSkew: 16,
	}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	_, err = io.ReadAll(resp.Body)
	require.Error(t, err)
	assert.ErrorIs(t, err, errNoRangeSupport)
	assert.Contains(t, err.Error(), "server resumed at byte")
	assert.Len(t, server.seenRanges(), 2)
}

func TestResumableTransport_refusesAResumeWithAnUnusableContentRange(t *testing.T) {
	// a server that cannot form a Content-Range will not form one on the fifth attempt either
	server := &flakyBlobServer{
		payload:           payloadOfSize(8192),
		dropsLeft:         1000,
		bytesPerDrop:      512,
		blankContentRange: true,
	}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	_, err = io.ReadAll(resp.Body)
	require.Error(t, err)
	assert.ErrorIs(t, err, errNoRangeSupport)
	assert.Len(t, server.seenRanges(), 2)
}

func TestResumableTransport_refusesAResumeWhenTheBlobLengthChanged(t *testing.T) {
	// a Content-Range reporting a different total is direct evidence the object is not the one the
	// read began with, and costs nothing to check
	server := &flakyBlobServer{
		payload:      payloadOfSize(8192),
		dropsLeft:    1,
		bytesPerDrop: 1000,
		totalSkew:    64,
	}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	_, err = io.ReadAll(resp.Body)
	require.Error(t, err)
	assert.ErrorIs(t, err, errObjectChanged)
	assert.NotErrorIs(t, err, errNoRangeSupport)
	assert.Len(t, server.seenRanges(), 2)
}

func TestResumableTransport_refusesAnEncodedSegment(t *testing.T) {
	// splicing decoded bytes onto a stream of wire bytes would be caught by a blob's digest but by
	// nothing at all on a wrapped manifest
	server := &flakyBlobServer{
		payload:         payloadOfSize(8192),
		dropsLeft:       1,
		bytesPerDrop:    1000,
		contentEncoding: "gzip",
	}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	_, err = io.ReadAll(resp.Body)
	require.Error(t, err)
	assert.ErrorIs(t, err, errNoRangeSupport)
	assert.Contains(t, err.Error(), "came back encoded")
}

func TestResumableTransport_asksForIdentityOnResume(t *testing.T) {
	// net/http would otherwise add gzip to the reissued request and transparently decode the reply
	server := &flakyBlobServer{payload: payloadOfSize(8192), dropsLeft: 1, bytesPerDrop: 1000}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)

	headers := server.seenHeaders()
	require.Len(t, headers, 2)
	assert.Equal(t, "identity", headers[1].Get("Accept-Encoding"), "the resume must pin the coding")
}

func TestResumableTransport_preservesRequestHeadersAcrossAResume(t *testing.T) {
	// Authorization and User-Agent are applied by transports above this one, so a resumed request
	// keeps them only because the original request is cloned
	server := &flakyBlobServer{payload: payloadOfSize(8192), dropsLeft: 2, bytesPerDrop: 1000}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer sekrit")
	req.Header.Set("User-Agent", "stereoscope-test")

	resp, err := newTestClient(1).Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)

	headers := server.seenHeaders()
	require.Len(t, headers, 3)
	for i, h := range headers {
		assert.Equal(t, "Bearer sekrit", h.Get("Authorization"), "request %d lost its credentials", i)
		assert.Equal(t, "stereoscope-test", h.Get("User-Agent"), "request %d lost its user agent", i)
	}
}

func TestResumableTransport_resumesThroughARedirectToACDN(t *testing.T) {
	// mcr.microsoft.com answers the blob request with a 307 to a CDN, and it is the CDN hop that
	// drops. go-containerregistry follows the redirect through this transport, so the resumed
	// request must go to the CDN rather than back to the registry
	payload := payloadOfSize(8192)
	cdn := &flakyBlobServer{payload: payload, dropsLeft: 2, bytesPerDrop: 1000}

	cdnSrv := httptest.NewServer(cdn)
	t.Cleanup(cdnSrv.Close)

	registry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, cdnSrv.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(registry.Close)

	resp, err := newTestClient(1).Get(registry.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
	assert.Equal(t, []string{"", "bytes=1000-8191", "bytes=2000-8191"}, cdn.seenRanges(),
		"the resumes should go to the CDN, not the registry")
}

func TestResumableTransport_neverReturnsMoreThanWasDeclared(t *testing.T) {
	// a 206 whose Content-Range checks out but whose body runs past the end of the range must not be
	// spliced in whole: an unwrapped net/http body caps at Content-Length, and a manifest above
	// resumeMinSize is wrapped here without a digest above it to catch the surplus.
	//
	// the segment is served chunked, so there is no Content-Length to cross-check against the
	// Content-Range and the cap in Read is the only thing withholding the surplus. a segment that
	// does declare a length inconsistent with its range is refused outright instead, which
	// TestResumableTransport_refusesASegmentLongerThanTheRangeItClaims covers
	payload := payloadOfSize(8192)
	overrun := payloadOfSize(500)

	var mu sync.Mutex
	served := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		served++
		first := served == 1
		mu.Unlock()

		if first {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload[:1000])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}

		start, _ := parseRangeRequest(r.Header.Get("Range"))
		tail := append(append([]byte{}, payload[start:]...), overrun...)

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(payload)-1, len(payload)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(tail)
	}))
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Len(t, got, len(payload), "the surplus past Content-Length must be withheld")
	assert.Equal(t, payload, got)
}

func TestResumableTransport_resumesOverHTTP2(t *testing.T) {
	// every registry in the probe table serves HTTP/2, where a truncated body surfaces through a
	// different path in net/http than it does over HTTP/1.1
	payload := payloadOfSize(2 * 1024 * 1024)
	server := &flakyBlobServer{payload: payload, dropsLeft: 3, bytesPerDrop: 200 * 1024}

	srv := httptest.NewUnstartedServer(server)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	transport := newResumableTransport(srv.Client().Transport)
	transport.minSize = 1
	transport.backoff = 0

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	require.Equal(t, "HTTP/2.0", resp.Proto, "the fixture must actually be serving HTTP/2")
	require.IsType(t, &resumableBody{}, resp.Body)

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
	assert.Len(t, server.seenRanges(), 4)
}

func TestResumableTransport_completesWhenTheConnectionIsResetAfterTheLastByte(t *testing.T) {
	// over HTTP/2 a RST_STREAM following the final DATA frame surfaces as a stream error rather
	// than io.EOF; resuming from there would ask for a range past the last byte
	payload := payloadOfSize(2 * 1024 * 1024)
	server := &flakyBlobServer{payload: payload, abortAfterBody: true}

	srv := httptest.NewUnstartedServer(server)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	transport := newResumableTransport(srv.Client().Transport)
	transport.minSize = 1
	transport.backoff = 0

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
	assert.Len(t, server.seenRanges(), 1, "a complete body must not be resumed at all")
}

func TestResumableTransport_readAfterCloseDoesNotRestartTheDownload(t *testing.T) {
	server := &flakyBlobServer{payload: payloadOfSize(8192)}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)

	require.IsType(t, &resumableBody{}, resp.Body, "the fixture must produce a wrapped body")

	buf := make([]byte, 16)
	_, err = resp.Body.Read(buf)
	require.NoError(t, err)

	before := len(server.seenRanges())
	require.NoError(t, resp.Body.Close())

	n, err := resp.Body.Read(buf)
	assert.Zero(t, n)
	require.Error(t, err, "a closed body must not go on serving bytes")
	assert.Len(t, server.seenRanges(), before, "a read after close must not open a new connection")
}

func TestResumableTransport_closeAbandonsABackoffInProgress(t *testing.T) {
	// zero bytes per attempt means the stall count climbs and the backoff genuinely engages
	server := &flakyBlobServer{payload: payloadOfSize(1 << 20), dropsLeft: 10_000, bytesPerDrop: 0}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	transport := newTestTransport(1)
	transport.backoff = 10 * time.Second
	transport.maxStalls = 1_000_000

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)

	returned := make(chan struct{})
	go func() {
		_, _ = io.ReadAll(resp.Body)
		close(returned)
	}()

	// the first attempt goes out immediately, the second waits a full backoff
	time.Sleep(300 * time.Millisecond)
	atClose := len(server.seenRanges())
	require.Greater(t, atClose, 1, "the fixture must have reached a backoff for this test to mean anything")

	closedAt := time.Now()
	require.NoError(t, resp.Body.Close())

	select {
	case <-returned:
		assert.Less(t, time.Since(closedAt), 2*time.Second,
			"Close must abandon the backoff, which here would otherwise run for 10s")
	case <-time.After(5 * time.Second):
		t.Fatal("the reader never returned after Close")
	}

	time.Sleep(300 * time.Millisecond)
	assert.LessOrEqual(t, len(server.seenRanges()), atClose,
		"a closed body must not issue a further request on its way out")
}

func TestResumableTransport_doesNotResumeACancelledRequest(t *testing.T) {
	server := &flakyBlobServer{payload: payloadOfSize(8192), dropsLeft: 1000, bytesPerDrop: 512}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	require.NoError(t, err)

	resp, err := newTestClient(1).Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	cancel()

	_, err = io.ReadAll(resp.Body)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled, "cancellation should surface as such, not as a clean EOF")

	before := len(server.seenRanges())
	_, again := resp.Body.Read(make([]byte, 16))
	assert.ErrorIs(t, again, context.Canceled, "the terminal error should be latched")
	assert.Len(t, server.seenRanges(), before, "reading past a terminal error must not reopen anything")
}

func TestResumableTransport_leavesSmallResponsesAlone(t *testing.T) {
	// below the threshold a response arrives in too few segments for a mid-body drop to be a
	// realistic failure, so manifests, configs and auth tokens stay clear of the machinery
	server := &flakyBlobServer{payload: payloadOfSize(8192)}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(resumeMinSize).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	_, ok := resp.Body.(*resumableBody)
	assert.False(t, ok, "a response below the size threshold should not be wrapped")
}

func TestResumableTransport_leavesUnknownLengthResponsesAlone(t *testing.T) {
	// without a Content-Length there is no way to tell a truncated stream from a complete one, so
	// such a body is passed through untouched rather than resumed on a guess
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// flushing before the first write forces chunked encoding, so no Content-Length is sent
		w.(http.Flusher).Flush()
		_, _ = w.Write(payloadOfSize(2048))
	}))
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	require.Equal(t, int64(-1), resp.ContentLength, "the fixture must not send a Content-Length")

	_, ok := resp.Body.(*resumableBody)
	assert.False(t, ok, "a response of unknown length should not be wrapped")
}

func TestResumableTransport_shouldResume(t *testing.T) {
	get := func(rangeHeader string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://example.com/v2/img/blobs/sha256:abc", nil)
		if rangeHeader != "" {
			req.Header.Set("Range", rangeHeader)
		}
		return req
	}

	response := func(status int, length int64) *http.Response {
		return &http.Response{
			StatusCode:    status,
			ContentLength: length,
			Body:          io.NopCloser(strings.NewReader("")),
			Header:        http.Header{},
		}
	}

	encoded := func(coding string) *http.Response {
		resp := response(http.StatusOK, resumeMinSize)
		resp.Header.Set("Content-Encoding", coding)
		return resp
	}

	transport := newResumableTransport(http.DefaultTransport)

	tests := []struct {
		name     string
		req      *http.Request
		resp     *http.Response
		expected bool
	}{
		{
			name:     "a large plain GET is wrapped",
			req:      get(""),
			resp:     response(http.StatusOK, resumeMinSize),
			expected: true,
		},
		{
			// http.Transport never returns (nil, nil), but newResumableTransport takes any
			// RoundTripper, and a base that broke the contract used to panic here rather than
			// simply not being wrapped
			name:     "a nil response is left alone",
			req:      get(""),
			resp:     nil,
			expected: false,
		},
		{
			// the offsets are wire bytes, so an identity segment cannot be spliced onto a stream
			// that arrived in some coding. net/http leaves an explicit one from the server in place
			name:     "an encoded response is left alone",
			req:      get(""),
			resp:     encoded("br"),
			expected: false,
		},
		{
			name:     "an identity coding is treated as no coding at all",
			req:      get(""),
			resp:     encoded("identity"),
			expected: true,
		},
		{
			name:     "a response just below the threshold is left alone",
			req:      get(""),
			resp:     response(http.StatusOK, resumeMinSize-1),
			expected: false,
		},
		{
			name:     "an unknown content length is left alone",
			req:      get(""),
			resp:     response(http.StatusOK, -1),
			expected: false,
		},
		{
			name:     "a caller's own range request is left alone",
			req:      get("bytes=10-"),
			resp:     response(http.StatusOK, resumeMinSize),
			expected: false,
		},
		{
			name:     "a non-200 response is left alone",
			req:      get(""),
			resp:     response(http.StatusNotFound, resumeMinSize),
			expected: false,
		},
		{
			name:     "a HEAD is left alone",
			req:      httptest.NewRequest(http.MethodHead, "http://example.com/v2/img/blobs/sha256:abc", nil),
			resp:     response(http.StatusOK, resumeMinSize),
			expected: false,
		},
		{
			name:     "a response with no body is left alone",
			req:      get(""),
			resp:     &http.Response{StatusCode: http.StatusOK, ContentLength: resumeMinSize},
			expected: false,
		},
		{
			name:     "a request carrying a body is left alone",
			req:      httptest.NewRequest(http.MethodGet, "http://example.com/v2/img/blobs/sha256:abc", strings.NewReader("x")),
			resp:     response(http.StatusOK, resumeMinSize),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, transport.shouldResume(tt.req, tt.resp))
		})
	}
}

func TestParseContentRange(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		start   int64
		end     int64
		total   int64
		wantErr bool
	}{
		{name: "a well formed range", header: "bytes 4096-8191/16384", start: 4096, end: 8191, total: 16384},
		{name: "a range starting at zero", header: "bytes 0-15/16", start: 0, end: 15, total: 16},
		{name: "an unknown total", header: "bytes 512-1023/*", start: 512, end: 1023, total: -1},
		{name: "extra whitespace", header: "  bytes  4096-8191/16384 ", start: 4096, end: 8191, total: 16384},
		{name: "an unparseable last byte is advisory", header: "bytes 4096-/16384", start: 4096, end: -1, total: 16384},
		{name: "an empty header", header: "", wantErr: true},
		{name: "a missing unit", header: "4096-8191/16384", wantErr: true},
		{name: "a unit that is not bytes", header: "items 0-5/10", wantErr: true},
		{name: "a missing total", header: "bytes 4096-8191", wantErr: true},
		{name: "a non-numeric start", header: "bytes abc-8191/16384", wantErr: true},
		{name: "a non-numeric total", header: "bytes 4096-8191/many", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, total, err := parseContentRange(tt.header)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.start, start)
			assert.Equal(t, tt.end, end)
			assert.Equal(t, tt.total, total)
		})
	}
}

func TestResumableTransport_retriesAResumeTheServerCouldNotServeYet(t *testing.T) {
	// a CDN edge unhealthy enough to sever the stream is exactly the one liable to answer the
	// reopen with a 503. treating that as "this server cannot resume" discarded everything already
	// delivered, while a dropped connection from the same host was survivable: the asymmetry was
	// not intended.
	payload := payloadOfSize(8192)
	server := &flakyBlobServer{
		payload:         payload,
		dropsLeft:       1,
		bytesPerDrop:    1000,
		transientStatus: http.StatusServiceUnavailable,
		transientTimes:  2,
	}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "the blob must be reassembled across the refusals")

	// the initial request, the two refusals, and the attempt that finally served the range
	assert.Len(t, server.seenRanges(), 4)
}

func TestResumableTransport_spendsTheBudgetWhenTheServerStaysUnavailable(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := &flakyBlobServer{
				payload:         payloadOfSize(8192),
				dropsLeft:       1,
				bytesPerDrop:    1000,
				transientStatus: status,
			}

			srv := httptest.NewServer(server)
			t.Cleanup(srv.Close)

			resp, err := newTestClient(1).Get(srv.URL)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })

			_, err = io.ReadAll(resp.Body)
			require.Error(t, err)
			assert.ErrorIs(t, err, errResumeUnavailable)
			assert.NotErrorIs(t, err, errNoRangeSupport,
				"a busy server must not be reported as one that cannot resume")
			assert.Contains(t, err.Error(), "without progress")

			// the whole budget is spent rather than abandoned on the first refusal
			assert.Len(t, server.seenRanges(), maxConsecutiveStalls+1)
		})
	}
}

func TestResumableTransport_refusesAResumeWhoseCredentialsWereRejected(t *testing.T) {
	// reopen reissues the request below go-containerregistry's auth transport, so every attempt
	// would carry the same expired token and earn the same refusal. this stays permanent on
	// purpose; only the diagnosis changes, because blaming range support sent operators hunting
	// for a registry limitation that does not exist.
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := &flakyBlobServer{
				payload:         payloadOfSize(8192),
				dropsLeft:       1,
				bytesPerDrop:    1000,
				transientStatus: status,
			}

			srv := httptest.NewServer(server)
			t.Cleanup(srv.Close)

			resp, err := newTestClient(1).Get(srv.URL)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })

			_, err = io.ReadAll(resp.Body)
			require.Error(t, err)
			assert.ErrorIs(t, err, errCredentialsRejected)
			assert.NotErrorIs(t, err, errNoRangeSupport)
			assert.NotErrorIs(t, err, errResumeUnavailable)
			assert.Contains(t, err.Error(), "rejected the credentials")

			// permanent, so it costs one attempt rather than the whole budget
			assert.Len(t, server.seenRanges(), 2)
		})
	}
}

func TestResumableTransport_acceptsAnIdentityContentEncoding(t *testing.T) {
	// reopen asks for identity, and a server that echoes the Accept-Encoding it was sent returns
	// it. identity is not a valid content-coding for a response, so it means the same as no header
	// at all; refusing it would permanently poison resuming against such a server.
	payload := payloadOfSize(8192)
	server := &flakyBlobServer{
		payload:         payload,
		dropsLeft:       1,
		bytesPerDrop:    1000,
		contentEncoding: "identity",
	}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got)
}

func Test_identityEncoded(t *testing.T) {
	assert.True(t, identityEncoded(""))
	assert.True(t, identityEncoded("identity"))
	assert.True(t, identityEncoded("Identity"))
	assert.True(t, identityEncoded("  identity  "))
	assert.False(t, identityEncoded("gzip"))
	assert.False(t, identityEncoded("identity, gzip"))
}

func TestResumableTransport_givesUpOnceTheTotalBudgetIsSpent(t *testing.T) {
	// every attempt delivers enough to replenish the stall budget, so only the total one can end
	// this. a path that drops this often is broken rather than flaky, and failing loudly is the
	// behaviour stereoscope had before this transport existed.
	payload := payloadOfSize(4 * 1024 * 1024)
	server := &flakyBlobServer{payload: payload, dropsLeft: 1000, bytesPerDrop: minResumeProgress}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	_, err = io.ReadAll(resp.Body)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "in total")

	// the initial request plus exactly the total budget, and no more
	assert.Len(t, server.seenRanges(), maxTotalResumes+1)
}

func TestResumableTransport_retriesABusyServerWhoseErrorPageIsEncoded(t *testing.T) {
	// the coding check guards a body about to be spliced, which is only ever a 206. running it
	// ahead of the status check made a busy server serving a compressed error page indistinguishable
	// from one that cannot resume at all, and that verdict is permanent.
	payload := payloadOfSize(8192)
	server := &flakyBlobServer{
		payload:                  payload,
		dropsLeft:                1,
		bytesPerDrop:             1000,
		transientStatus:          http.StatusServiceUnavailable,
		transientTimes:           2,
		transientContentEncoding: "gzip",
	}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, payload, got, "a compressed error page must not be read as a refusal to resume")

	// the initial request, the two refusals, and the attempt that finally served the range
	assert.Len(t, server.seenRanges(), 4)
}

func TestResumableTransport_closeAbortsAReopenInFlight(t *testing.T) {
	// closing a response body to abandon a read is documented usage, and an unwrapped net/http body
	// unblocks the reader at once. a reopen that has not yet returned is the one place the wrapper
	// could not reach by closing the current body, since there is no body yet.
	payload := payloadOfSize(2 * 1024 * 1024)

	var once sync.Once
	reopening := make(chan struct{})
	release := make(chan struct{})
	served := 0
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		served++
		first := served == 1
		mu.Unlock()

		if first {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload[:1024*1024])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}

		// accept the reopen and fall silent, never sending headers. released at cleanup rather than
		// blocked for ever, so the handler goroutine and the server both go away with the test
		once.Do(func() { close(reopening) })
		<-release
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)

	read := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(resp.Body)
		read <- readErr
	}()

	<-reopening
	require.NoError(t, resp.Body.Close())

	select {
	case readErr := <-read:
		assert.ErrorIs(t, readErr, http.ErrBodyReadAfterClose)
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not release a reader blocked on a reopen that never answered")
	}
}

func TestResumableTransport_refusesASegmentLongerThanTheRangeItClaims(t *testing.T) {
	// a server with no real range support may echo back the range it was asked for and then serve
	// the object from the beginning anyway. it contradicts itself doing so -- the body is longer
	// than the span the Content-Range claims -- and that is worth catching here, because otherwise
	// the whole layer downloads again before the digest check rejects it.
	payload := payloadOfSize(8192)
	server := &flakyBlobServer{
		payload:        payload,
		dropsLeft:      1,
		bytesPerDrop:   1000,
		restartOnRange: true,
	}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	got, err := io.ReadAll(resp.Body)
	require.Error(t, err)
	assert.ErrorIs(t, err, errNoRangeSupport)
	assert.Contains(t, err.Error(), "covers")
	assert.Len(t, got, 1000, "only the bytes from before the drop are handed back")

	// permanent, so it costs one attempt rather than the whole budget
	assert.Len(t, server.seenRanges(), 2)
}

func TestResumableTransport_endsAReadThatStalls(t *testing.T) {
	// the failure this bounds is a body that stops arriving while the connection stays open, so the
	// server is never released until cleanup: if the watchdog does not end the read, nothing will
	payload := payloadOfSize(64 * 1024)
	server := &stallingBlobServer{payload: payload, stallAfter: 1024, release: make(chan struct{})}

	srv := httptest.NewServer(server)
	t.Cleanup(func() { close(server.release); srv.Close() })

	transport := newTestTransport(1)
	transport.stallTimeout = 100 * time.Millisecond

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	type result struct {
		body []byte
		err  error
	}

	// read on another goroutine and time out here: a regression in the watchdog leaves the read
	// blocked for ever, and asserting on elapsed time after the fact cannot catch that -- the
	// assertion is never reached. this way the test fails by name in a second instead of panicking
	// at go test's ten minute default with a goroutine dump
	done := make(chan result, 1)
	go func() {
		body, err := io.ReadAll(resp.Body)
		done <- result{body: body, err: err}
	}()

	select {
	case got := <-done:
		require.NoError(t, got.err)
		assert.Equal(t, payload, got.body, "the stalled prefix should be spliced onto the resumed remainder")
		assert.Equal(t, []string{"", "bytes=1024-65535"}, server.seenRanges(),
			"the resume should ask for exactly the bytes the stalled attempt did not deliver")
	case <-time.After(10 * time.Second):
		t.Fatal("the watchdog never ended the stalled read: the body is still blocked")
	}
}

func TestResumableTransport_leavesASlowButAdvancingBodyAlone(t *testing.T) {
	// guards the constant against being read as a clock on slowness: this body takes far longer than
	// the stall timeout to arrive in total, but is never silent for as long as one
	payload := payloadOfSize(32 * 1024)
	server := &pacedBlobServer{payload: payload, writes: 16, gap: 5 * time.Millisecond}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	transport := newTestTransport(1)
	// a hundredfold margin rather than a fivefold one: at 5x, scheduling noise under -race fired the
	// watchdog about once in ninety runs, and the resume then met a fixture that ignores Range and
	// failed the test outright. what is being asserted is the ratio, so the absolute values are free
	transport.stallTimeout = 500 * time.Millisecond

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, payload, got)
	assert.Equal(t, 1, server.attemptCount(), "a body that keeps delivering should never be resumed")
}

func TestResumableTransport_stallDeadlineIsOffWhenUnset(t *testing.T) {
	// the zero value of stallTimeout is the master switch: it turns off the first-byte budget with
	// it, so a caller that wants no watchdog at all gets none rather than a minute-long one.
	//
	// the fixture pauses two hundred times longer than the first-byte budget it is given, so this
	// test fails the moment that budget is armed at all -- which is what an earlier version of it
	// could not do, since its server answered instantly and no budget was ever reached
	payload := payloadOfSize(8192)
	server := &pausingBlobServer{payload: payload, prefix: 512, pause: 200 * time.Millisecond}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	transport := newTestTransport(1)
	transport.stallTimeout = 0

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, payload, got)
	assert.Equal(t, 1, server.attemptCount(),
		"with the watchdog off the pause should be waited out, not resumed around")
}

func TestResumableTransport_releasesTheContextOfAnUnwrappedBody(t *testing.T) {
	// a response too small to wrap still carries a context minted by RoundTrip. closing its body has
	// to release it, or every manifest fetch leaks one until the pull's own context is done
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("too small to wrap"))
	}))
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1 << 20).Get(srv.URL)
	require.NoError(t, err)

	wrapped, ok := resp.Body.(*cancelOnClose)
	require.True(t, ok, "an unwrapped body should still carry its cancel")

	released := make(chan struct{})
	original := wrapped.cancel
	wrapped.cancel = func() { original(); close(released) }

	require.NoError(t, resp.Body.Close())

	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("closing the body should have released the context minted for it")
	}
}

// clampingBlobServer honours every Range but answers with a self-consistent 206 far shorter than
// the span asked for: Content-Range, Content-Length and the bytes written all agree, so the segment
// is accepted and then ends in a bare io.EOF well short of the object. It is the shape a badly
// configured mirror takes, and the only one that produces an io.EOF as the cause of a terminal
// failure -- an ordinary truncation is reported by net/http as io.ErrUnexpectedEOF already.
type clampingBlobServer struct {
	payload []byte
	clamp   int
}

func (s *clampingBlobServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := int64(0)
	status := http.StatusOK

	if spec := r.Header.Get("Range"); spec != "" {
		var end int64
		if _, err := fmt.Sscanf(spec, "bytes=%d-%d", &start, &end); err != nil {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		status = http.StatusPartialContent

		sending := int64(s.clamp)
		if start+sending > int64(len(s.payload)) {
			sending = int64(len(s.payload)) - start
		}

		w.Header().Set("Content-Range",
			fmt.Sprintf("bytes %d-%d/%d", start, start+sending-1, len(s.payload)))
		w.Header().Set("Content-Length", strconv.FormatInt(sending, 10))
		w.WriteHeader(status)
		_, _ = w.Write(s.payload[start : start+sending])

		return
	}

	// the first response declares the whole object and delivers a fraction of it
	w.Header().Set("Content-Length", strconv.Itoa(len(s.payload)))
	w.WriteHeader(status)
	_, _ = w.Write(s.payload[:s.clamp])
	w.(http.Flusher).Flush()
}

func TestResumableTransport_aFailedTransferIsNeverACleanEndOfStream(t *testing.T) {
	// the sharpest edge this transport could leave: a caller testing errors.Is(err, io.EOF) to mean
	// "the stream ended" would read a truncated layer as a complete one. file.IterateTar in this
	// repo does exactly that test, so the terminal error must never carry io.EOF in its chain
	payload := payloadOfSize(256 * 1024)
	server := &clampingBlobServer{payload: payload, clamp: 4096}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	resp, err := newTestClient(1).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	_, err = io.ReadAll(resp.Body)
	require.Error(t, err, "a transfer that never completes must fail")

	assert.NotErrorIs(t, err, io.EOF,
		"a truncation must not be mistakable for a clean end of stream")
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF,
		"it should be reported as the short read it is")
}

// pausingBlobServer writes its response headers at once and then waits before sending any body at
// all, which is what a pull-through cache or a scanning proxy does on a cold object.
type pausingBlobServer struct {
	payload []byte
	pause   time.Duration
	// prefix is delivered before the pause, so the pause is silence in the middle of a body rather
	// than a slow start -- which is the only thing the watchdog bounds
	prefix int

	mu       sync.Mutex
	attempts int
}

func (s *pausingBlobServer) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.attempts++
	s.mu.Unlock()

	w.Header().Set("Content-Length", strconv.Itoa(len(s.payload)))
	w.WriteHeader(http.StatusOK)

	if s.prefix > 0 {
		_, _ = w.Write(s.payload[:s.prefix])
	}

	w.(http.Flusher).Flush()

	time.Sleep(s.pause)

	_, _ = w.Write(s.payload[s.prefix:])
}

func (s *pausingBlobServer) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func TestResumableTransport_waitsForASlowFirstByteRatherThanBoundingIt(t *testing.T) {
	// a slow start is not a stall. bounding it fails a pull that would have succeeded, and fails it
	// hard: nothing has been delivered, so nothing replenishes the stall budget, and every attempt
	// is a full restart that makes the server's own fetch begin again. the pause here is many times
	// the mid-body budget, and must be waited out rather than resumed around
	payload := payloadOfSize(32 * 1024)
	server := &pausingBlobServer{payload: payload, pause: 150 * time.Millisecond}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	transport := newTestTransport(1)
	transport.stallTimeout = 5 * time.Millisecond

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, payload, got)
	assert.Equal(t, 1, server.attemptCount(),
		"a slow first byte should be waited for, not resumed around")
}

// silentReopenServer serves a prefix and drops, then accepts every later connection without ever
// writing response headers, which is what an endpoint that is up but wedged looks like.
type silentReopenServer struct {
	payload    []byte
	firstBytes int
	release    chan struct{}

	mu       sync.Mutex
	attempts int
}

func (s *silentReopenServer) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.attempts++
	attempt := s.attempts
	s.mu.Unlock()

	if attempt > 1 {
		<-s.release
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(s.payload)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(s.payload[:s.firstBytes])
	w.(http.Flusher).Flush()
	panic(http.ErrAbortHandler)
}

func TestResumableTransport_boundsAReopenThatNeverSendsHeaders(t *testing.T) {
	// the header clock was a package constant until it became a field, which left this branch with
	// no coverage at all and no way for an operator to shorten a thirty second wait per attempt
	payload := payloadOfSize(64 * 1024)
	server := &silentReopenServer{payload: payload, firstBytes: 1024, release: make(chan struct{})}

	srv := httptest.NewServer(server)
	t.Cleanup(func() { close(server.release); srv.Close() })

	transport := newTestTransport(1)
	transport.headerTimeout = 50 * time.Millisecond

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	done := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(resp.Body)
		done <- readErr
	}()

	select {
	case readErr := <-done:
		require.Error(t, readErr)
		assert.Contains(t, readErr.Error(), "no response headers within",
			"the failure should name the clock that ended it, not a bare cancellation")
		assert.NotErrorIs(t, readErr, context.Canceled,
			"our own timer must not reach the caller as a cancellation nobody asked for")
	case <-time.After(30 * time.Second):
		t.Fatal("a reopen that never sent headers was never bounded")
	}
}

// acceptAndCloseListener serves one truncated blob over a raw socket and then answers every later
// connection by reading the request and closing without a response. That is what a load balancer
// or CDN edge whose backend pool has drained does, and net/http reports it to the caller of
// RoundTrip as a bare io.EOF -- the second door into the terminal error, which httptest cannot
// produce because it always writes a response.
type acceptAndCloseListener struct {
	listener net.Listener
	payload  []byte
	prefix   int

	mu       sync.Mutex
	attempts int
}

func newAcceptAndCloseListener(t *testing.T, payload []byte, prefix int) *acceptAndCloseListener {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	server := &acceptAndCloseListener{listener: listener, payload: payload, prefix: prefix}
	go server.serve()

	t.Cleanup(func() { _ = listener.Close() })

	return server
}

func (s *acceptAndCloseListener) url() string {
	return "http://" + s.listener.Addr().String() + "/blob"
}

func (s *acceptAndCloseListener) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}

		go s.handle(conn)
	}
}

func (s *acceptAndCloseListener) handle(conn net.Conn) {
	defer conn.Close()

	s.mu.Lock()
	s.attempts++
	attempt := s.attempts
	s.mu.Unlock()

	// read the request line and headers, so the client has genuinely written its request
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil || line == "\r\n" {
			break
		}
	}

	if attempt > 1 {
		// accept, read, and close with no response at all
		return
	}

	fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", len(s.payload))
	_, _ = conn.Write(s.payload[:s.prefix])
}

func TestResumableTransport_aReopenThatIsNeverAnsweredIsNotACleanEndOfStream(t *testing.T) {
	// the sibling of aFailedTransferIsNeverACleanEndOfStream, and the door it does not cover: there
	// the io.EOF comes from the body read, here it comes from RoundTrip itself. the invariant is the
	// same and has to hold on both, or a caller testing errors.Is(err, io.EOF) reads a layer we
	// failed to fetch as one that simply ended
	payload := payloadOfSize(64 * 1024)
	server := newAcceptAndCloseListener(t, payload, 1024)

	transport := newTestTransport(1)
	transport.headerTimeout = time.Second

	resp, err := (&http.Client{Transport: transport}).Get(server.url())
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	got, err := io.ReadAll(resp.Body)
	require.Error(t, err, "a transfer whose every reopen goes unanswered must fail")

	assert.Len(t, got, 1024, "the bytes that did arrive should still be handed over")
	assert.NotErrorIs(t, err, io.EOF,
		"a reopen that was never answered must not be mistakable for a clean end of stream")
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF,
		"it should be reported as the short read it is")
}

func TestResumableTransport_endsAReadThatStallsOverHTTP2(t *testing.T) {
	// the stall arrives through a different path in net/http on HTTP/2 than on HTTP/1.1, and every
	// registry in the probe table serves HTTP/2. the suite had an HTTP/2 drop test but no stall one
	payload := payloadOfSize(64 * 1024)
	server := &stallingBlobServer{payload: payload, stallAfter: 1024, release: make(chan struct{})}

	srv := httptest.NewUnstartedServer(server)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(func() { close(server.release); srv.Close() })

	transport := newResumableTransport(srv.Client().Transport)
	transport.minSize = 1
	transport.backoff = 0
	transport.stallTimeout = 100 * time.Millisecond

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	require.Equal(t, "HTTP/2.0", resp.Proto, "the fixture must actually be serving HTTP/2")

	type result struct {
		body []byte
		err  error
	}

	done := make(chan result, 1)
	go func() {
		body, readErr := io.ReadAll(resp.Body)
		done <- result{body: body, err: readErr}
	}()

	select {
	case got := <-done:
		require.NoError(t, got.err)
		assert.Equal(t, payload, got.body)
	case <-time.After(10 * time.Second):
		t.Fatal("the watchdog never ended the stalled HTTP/2 read")
	}
}

// shortThenErrTransport hands back a body whose single Read returns bytes AND an error together,
// then honours ranges. net/http's own body does exactly this on a mid-stream drop, and no httptest
// fixture can produce it: a handler that aborts yields (0, err) on the following read instead. The
// bytes delivered alongside the error have already been counted into the offset, so a resume that
// discards them silently truncates the stream.
type shortThenErrTransport struct {
	payload []byte
	prefix  int

	mu     sync.Mutex
	served int
}

type shortThenErrBody struct {
	data []byte
	done bool
}

func (b *shortThenErrBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.ErrUnexpectedEOF
	}

	b.done = true
	n := copy(p, b.data)

	return n, io.ErrUnexpectedEOF
}

func (b *shortThenErrBody) Close() error { return nil }

func (t *shortThenErrTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.mu.Lock()
	t.served++
	first := t.served == 1
	t.mu.Unlock()

	header := http.Header{}

	if first {
		return &http.Response{
			StatusCode:    http.StatusOK,
			ContentLength: int64(len(t.payload)),
			Header:        header,
			Body:          &shortThenErrBody{data: t.payload[:t.prefix]},
			Request:       req,
		}, nil
	}

	var start, end int64
	if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
		return nil, err
	}

	header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(t.payload)))

	return &http.Response{
		StatusCode:    http.StatusPartialContent,
		ContentLength: end - start + 1,
		Header:        header,
		Body:          io.NopCloser(bytes.NewReader(t.payload[start : end+1])),
		Request:       req,
	}, nil
}

func TestResumableTransport_keepsBytesDeliveredAlongsideTheErrorThatEndedThem(t *testing.T) {
	// the realistic drop shape: a read that returns n > 0 and an error together. those bytes are
	// already counted into the offset, so the resume continues past them -- if they were not also
	// handed to the caller the stream would be short by exactly that much, with no error at all
	payload := payloadOfSize(64 * 1024)
	base := &shortThenErrTransport{payload: payload, prefix: 512}

	transport := newResumableTransport(base)
	transport.minSize = 1
	transport.backoff = 0

	resp, err := (&http.Client{Transport: transport}).Get("http://example.invalid/blob")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resp.Body.Close()) })

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Len(t, got, len(payload), "no byte delivered before the error may be dropped")
	assert.Equal(t, payload, got)
}

// perpetuallyStallingServer never sends a body byte on any attempt, so the read ends on the
// watchdog every time and the terminal error is whatever the stall path decided to call it.
type perpetuallyStallingServer struct {
	payload []byte
	release chan struct{}
}

func (s *perpetuallyStallingServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	length := len(s.payload)
	status := http.StatusOK

	if spec := r.Header.Get("Range"); spec != "" {
		var start, end int64
		if _, err := fmt.Sscanf(spec, "bytes=%d-%d", &start, &end); err == nil {
			status = http.StatusPartialContent
			length = int(end - start + 1)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(s.payload)))
		}
	}

	w.Header().Set("Content-Length", strconv.Itoa(length))
	w.WriteHeader(status)

	// a byte first: silence before the first byte is slowness, not a stall, and is deliberately
	// not bounded. what is bounded is a body that starts and then stops
	_, _ = w.Write(s.payload[:1])
	w.(http.Flusher).Flush()
	<-s.release
}

func TestResumableTransport_aStallIsReportedAsAStall(t *testing.T) {
	// the watchdog ends a read by cancelling the request, and a cancellation left bare reaches the
	// caller as context.Canceled -- a Ctrl-C nobody pressed, which syft and grype are entitled to
	// treat as a user abort and swallow. the reopen path is asserted on elsewhere; this is the read
	payload := payloadOfSize(64 * 1024)
	server := &perpetuallyStallingServer{payload: payload, release: make(chan struct{})}

	srv := httptest.NewServer(server)
	t.Cleanup(func() { close(server.release); srv.Close() })

	transport := newTestTransport(1)
	transport.stallTimeout = 20 * time.Millisecond

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	done := make(chan error, 1)
	go func() {
		_, readErr := io.ReadAll(resp.Body)
		done <- readErr
	}()

	select {
	case readErr := <-done:
		require.Error(t, readErr)
		assert.ErrorIs(t, readErr, errReadStalled, "a server that says nothing has stalled")
		assert.NotErrorIs(t, readErr, context.Canceled,
			"the cancellation was ours, and must not read as the caller's")
		assert.NotErrorIs(t, readErr, io.EOF)
	case <-time.After(20 * time.Second):
		t.Fatal("a body that never delivered anything was never abandoned")
	}
}

// badRangeServer drops its first response mid-body and answers every resume with a Content-Range of
// the test's choosing, which is how the shapes acceptable refuses are reached.
type badRangeServer struct {
	payload      []byte
	prefix       int
	contentRange string
	// chunked omits Content-Length, which is the only thing that disables the span cross-check. a
	// shape meant to be caught by one of the position checks has to get past that one first, or the
	// test cannot tell which guard did the work
	chunked bool

	mu       sync.Mutex
	attempts int
}

func (s *badRangeServer) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.attempts++
	first := s.attempts == 1
	s.mu.Unlock()

	if first {
		w.Header().Set("Content-Length", strconv.Itoa(len(s.payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.payload[:s.prefix])
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	}

	remaining := s.payload[s.prefix:]
	w.Header().Set("Content-Range", s.contentRange)

	if !s.chunked {
		w.Header().Set("Content-Length", strconv.Itoa(len(remaining)))
	}

	w.WriteHeader(http.StatusPartialContent)
	_, _ = w.Write(remaining)
}

func TestResumableTransport_refusesAnUncheckableOrImpossibleContentRange(t *testing.T) {
	payload := payloadOfSize(64 * 1024)
	const prefix = 1024

	tests := []struct {
		name         string
		contentRange string
		chunked      bool
		expected     error
	}{
		{
			// RFC 9110 makes this malformed for a 206, and it is the one shape that switches off
			// both the span check and the changed-object check at once
			name:         "neither a last byte position nor a total",
			contentRange: fmt.Sprintf("bytes %d-*/*", prefix),
			expected:     errNoRangeSupport,
		},
		{
			name:         "a segment that ends before it starts",
			contentRange: fmt.Sprintf("bytes %d-%d/%d", prefix, prefix-10, len(payload)),
			chunked:      true,
			expected:     errNoRangeSupport,
		},
		{
			name:         "a segment that ends past the last byte of the object",
			contentRange: fmt.Sprintf("bytes %d-%d/%d", prefix, len(payload)+100, len(payload)),
			chunked:      true,
			expected:     errNoRangeSupport,
		},
		{
			// diagnosed as the object changing rather than as a server that cannot do ranges, which
			// is what an operator needs to read
			// the end is past the object as well, so both the position check and the changed-object
			// check fire. which one is reported is the whole point: an object that grew should be
			// diagnosed as one, not as a server that cannot do ranges
			name:         "a total that no longer matches the object the read began with",
			contentRange: fmt.Sprintf("bytes %d-%d/%d", prefix, len(payload)*2-1, len(payload)*2),
			expected:     errObjectChanged,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &badRangeServer{
				payload:      payload,
				prefix:       prefix,
				contentRange: test.contentRange,
				chunked:      test.chunked,
			}

			srv := httptest.NewServer(server)
			t.Cleanup(srv.Close)

			resp, err := newTestClient(1).Get(srv.URL)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })

			_, err = io.ReadAll(resp.Body)
			require.Error(t, err)
			assert.ErrorIs(t, err, test.expected)
			assert.NotErrorIs(t, err, io.EOF)
		})
	}
}

// noRangeServer stalls before its first byte and then, on every later attempt, answers with the
// whole object and no regard for any Range header -- a server with no range support at all.
type noRangeServer struct {
	payload []byte
	// restartLength, when set, is the length the restart declares and delivers instead of the whole
	// object; restartCoding, when set, is a Content-Encoding it claims for the restarted body
	restartLength int
	restartCoding string

	mu       sync.Mutex
	attempts int
	ranges   []string
}

func (s *noRangeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.attempts++
	first := s.attempts == 1
	s.ranges = append(s.ranges, r.Header.Get("Range"))
	s.mu.Unlock()

	body := s.payload
	if !first && s.restartLength > 0 {
		body = s.payload[:s.restartLength]
	}

	if !first && s.restartCoding != "" {
		w.Header().Set("Content-Encoding", s.restartCoding)
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	if first {
		// headers, then the connection dies before a single body byte: the wrapper is left at
		// offset zero with nothing to splice onto, which is what the restart path exists for
		panic(http.ErrAbortHandler)
	}

	_, _ = w.Write(body)
}

func (s *noRangeServer) seenRanges() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.ranges...)
}

func TestResumableTransport_restartsRatherThanRangesWhenNothingHasArrived(t *testing.T) {
	// a stall before the first byte used to reopen with Range: bytes=0-N, which gains nothing --
	// there is no prefix to splice onto -- while letting a server with no range support answer 200
	// and be classified as permanently unable to resume, failing a layer that would have arrived
	payload := payloadOfSize(64 * 1024)
	server := &noRangeServer{payload: payload}

	srv := httptest.NewServer(server)
	t.Cleanup(srv.Close)

	transport := newTestTransport(1)

	resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resp.Body.Close() })

	type result struct {
		body []byte
		err  error
	}

	done := make(chan result, 1)
	go func() {
		body, readErr := io.ReadAll(resp.Body)
		done <- result{body: body, err: readErr}
	}()

	select {
	case got := <-done:
		require.NoError(t, got.err, "a server with no range support should still be restartable")
		assert.Equal(t, payload, got.body)
		assert.Equal(t, []string{"", ""}, server.seenRanges(),
			"the restart must carry no Range, or this server answers 200 and is refused for ever")
	case <-time.After(20 * time.Second):
		t.Fatal("the restart never completed")
	}
}

func Test_forLog(t *testing.T) {
	// the only thing between a CDN-signed blob URL and an operator's log. both log sites and every
	// error message route through it
	parsed, err := url.Parse("https://cdn.example.com/v2/img/blobs/sha256:abc?sig=SECRET&se=2026-01-01")
	require.NoError(t, err)

	rendered := forLog(parsed)

	assert.Equal(t, "cdn.example.com/v2/img/blobs/sha256:abc", rendered)
	assert.NotContains(t, rendered, "SECRET", "a signed URL's credentials must never be rendered")
	assert.NotContains(t, rendered, "?")
}

func TestResumableTransport_checksTheRestartedBodyItAccepts(t *testing.T) {
	// the restart path skips the range checks because there is nothing to splice onto, which leaves
	// two guards carrying it alone. neither had a test, and losing either to a refactor would hand
	// the caller the wrong bytes on the one path with no digest above it to notice
	payload := payloadOfSize(64 * 1024)

	tests := []struct {
		name     string
		server   func() *noRangeServer
		expected error
	}{
		{
			name: "a restart of a different length is refused",
			server: func() *noRangeServer {
				return &noRangeServer{payload: payload, restartLength: len(payload) / 2}
			},
			expected: errObjectChanged,
		},
		{
			name: "a restart in a different coding is refused",
			server: func() *noRangeServer {
				return &noRangeServer{payload: payload, restartCoding: "gzip"}
			},
			expected: errNoRangeSupport,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := test.server()

			srv := httptest.NewServer(server)
			t.Cleanup(srv.Close)

			transport := newTestTransport(1)

			resp, err := (&http.Client{Transport: transport}).Get(srv.URL)
			require.NoError(t, err)
			t.Cleanup(func() { _ = resp.Body.Close() })

			done := make(chan error, 1)
			go func() {
				_, readErr := io.ReadAll(resp.Body)
				done <- readErr
			}()

			select {
			case readErr := <-done:
				require.Error(t, readErr)
				assert.ErrorIs(t, readErr, test.expected)
				assert.NotErrorIs(t, readErr, io.EOF,
					"a refused restart is not a clean end of stream either")
			case <-time.After(20 * time.Second):
				t.Fatal("the refused restart never ended the read")
			}
		})
	}
}
