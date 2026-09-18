package oci

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anchore/stereoscope/internal/log"
)

const (
	// resumeMinSize is the response size above which a body is worth wrapping. below roughly this
	// size a response arrives in too few segments for a mid-body drop to be a realistic failure,
	// and a request that fails before its body starts flowing is already retried by
	// go-containerregistry. it is a size predicate rather than a "blobs only" one, so an unusually
	// large manifest is wrapped too; gating on a /blobs/ path segment was rejected because the URL
	// actually read is the post-redirect CDN one, whose shape is the CDN's business.
	resumeMinSize = 1024 * 1024

	// maxConsecutiveStalls is how many attempts in a row may fail to deliver meaningful progress
	// before the read is abandoned. progress replenishes it, so a drop in the middle of an
	// otherwise healthy transfer costs nothing.
	maxConsecutiveStalls = 5

	// maxTotalResumes bounds the reopens for one body however much progress each one makes.
	// maxConsecutiveStalls cannot do this alone: any minResumeProgress delivered replenishes it, so
	// a path dropping every few kilobytes would reconnect indefinitely while technically advancing,
	// which on a multi-gigabyte layer reads to an operator as a hang rather than a failure.
	//
	// this is a safety net for the occasional drop, not a recovery mechanism for a broken path: a
	// pull needing more reopens than this should fail, and loudly, as it did before this transport
	// existed. it stays a comfortable multiple of maxConsecutiveStalls, since a single drop may
	// legitimately spend that whole budget on its own.
	maxTotalResumes = 20

	// minResumeProgress is how much must arrive to count as progress. any n > 0 is not enough: a
	// server delivering one byte per connection would otherwise replenish the budget for ever. the
	// surplus above it is discarded rather than banked, so each replenishment is earned afresh.
	minResumeProgress = 64 * 1024

	// reopenHeaderTimeout bounds how long a reopen may take to produce response headers. an
	// endpoint that accepts the connection and then falls silent is otherwise ended only by the
	// pull's context, which callers do not always give a deadline. it covers the headers only, so
	// a body that is arriving slowly is never on a clock.
	reopenHeaderTimeout = 30 * time.Second

	// firstByteTimeout is the budget for the very first byte of a transfer, which is a different
	// thing from the silence readStallTimeout bounds. a response can legitimately carry its headers
	// and Content-Length well before its first body byte -- a pull-through cache answers from
	// upstream metadata and only then fetches from origin -- and holding a cold start to the
	// mid-body budget would fail a pull that previously merely ran slowly.
	//
	// it covers the opening request only, not every attempt. a reopen goes to a host that has just
	// demonstrated it has the object positioned and streaming, and reopenHeaderTimeout already
	// bounds it reaching that point, so a resumed segment that then says nothing is silence rather
	// than cold start. that distinction is what keeps the worst case bounded: were this spent on
	// each of maxTotalResumes attempts, a server that trickles just enough to replenish the stall
	// budget and then goes quiet could hold a pull for the better part of an hour.
	//
	// these products run on servers and corporate estates, where a minute of silence before the
	// first byte means something is actually broken rather than merely slow, so the budget is sized
	// to tolerate a cold start once and not to wait out an outage.
	firstByteTimeout = time.Minute

	// readStallTimeout bounds how long a single read may wait for bytes that never come. an
	// unhealthy CDN edge does not close the connection when it stops serving a body: it simply goes
	// quiet, and the read blocks until the peer eventually errors, which measured around two minutes
	// against the registry that motivated this. reopenHeaderTimeout covers a reopen reaching the
	// server and nothing covers the transfer, so without this the resume machinery cannot learn that
	// anything is wrong until the peer decides to tell it.
	//
	// it applies once the transfer has delivered a byte; until then firstByteTimeout does. it is
	// armed around a single Read and disarmed the moment that Read returns, so it measures the
	// server's silence while we are actually waiting on it and never the caller's own think time.
	// that distinction is the point rather than an optimisation: the truncation this recovers from
	// is provoked by consuming slowly, so a clock over the transfer as a whole would abandon exactly
	// the readers it exists to protect. it is set well above any pause a healthy CDN takes mid-blob.
	readStallTimeout = 30 * time.Second

	// stallBackoff is multiplied by the consecutive stall count to space out repeated attempts. the
	// first attempt after a drop is made immediately, so an isolated drop costs no delay.
	stallBackoff = time.Second
)

// errNoRangeSupport marks a server that will not resume however often it is asked: it answered the
// range request with a status that settles the matter -- a plain 200, a 416, a redirect -- or with
// a 206 whose Content-Range is unusable or starts at the wrong byte, or with a body in a different
// content coding from the stream so far. a server that was merely busy is errResumeUnavailable,
// and one that refused to authorize the request is errCredentialsRejected.
var errNoRangeSupport = errors.New("server does not support resuming with range requests")

// errObjectChanged marks a resume whose Content-Range reports a different total length from the
// one the read began with, which is direct evidence the object is not the one we started.
var errObjectChanged = errors.New("the object changed while it was being read")

// errResumeUnavailable marks a reopen the server could not serve just now: it was rate limited or
// briefly broken. this is the condition resuming exists for -- a CDN edge unhealthy enough to
// sever a multi-gigabyte stream is exactly the one liable to answer the reopen with a 503 -- so it
// spends a stall and goes round again rather than discarding everything already delivered.
var errResumeUnavailable = errors.New("the server could not serve the resumed range just now")

// errCredentialsRejected marks a reopen the server refused to authorize. it is permanent here on
// purpose: reopen reissues the original request below go-containerregistry's auth transport, so
// every attempt would carry the same expired token or the same stale pre-signed URL and earn the
// same refusal. retrying would only turn one honest failure into ten. mending it properly means
// giving this transport a way to re-authorize, which is a larger change than resuming.
var errCredentialsRejected = errors.New("the server rejected the credentials on the resumed range request")

// errReadStalled marks a read the stall watchdog ended. it is a resume cause like any other: the
// bytes already delivered stand and the remainder is fetched on a fresh connection, so it is
// deliberately absent from permanentResumeError.
var errReadStalled = errors.New("the server stopped sending and the read stalled")

// resumableTransport wraps a RoundTripper so that a large GET whose body fails partway through is
// transparently resumed with a Range request from the byte already delivered.
//
// registries fronted by a CDN close the connection mid-blob on multi-gigabyte layers.
// go-containerregistry retries at the request level only, so once bytes are flowing a dropped
// connection surfaces as an unexpected EOF and the entire layer is discarded, however much of it
// had already arrived.
//
// this sits below go-containerregistry's verification wrapper, so for a blob the digest and size
// of the reassembled stream are still checked end to end: a resume that splices in the wrong bytes
// fails the layer exactly as a corrupt single-shot download would. that backstop is the blob
// path's and not a universal one -- a manifest fetched by tag is not digest-verified -- which is
// why the offset, length and coding of every resumed response are checked here as well.
//
// termination is two budgets and no more: consecutive attempts that deliver no meaningful
// progress, and reopens in total. an earlier revision carried five interacting ones, and each
// extra strangled some legitimate transfer before it was tuned back, so the pair is kept
// deliberately plain. between them they bound the number of attempts, and two clocks bound the
// waiting: one on a reopen producing response headers, one on a read producing bytes. neither is a
// clock on the transfer itself, which stays as slow as the caller wants it to be.
type resumableTransport struct {
	base          http.RoundTripper
	minSize       int64
	minProgress   int64
	backoff       time.Duration
	stallTimeout  time.Duration
	firstByte     time.Duration
	headerTimeout time.Duration
	maxStalls     int
	maxResumes    int
}

func newResumableTransport(base http.RoundTripper) *resumableTransport {
	if base == nil {
		base = http.DefaultTransport
	}

	return &resumableTransport{
		base:          base,
		minSize:       resumeMinSize,
		minProgress:   minResumeProgress,
		backoff:       stallBackoff,
		stallTimeout:  readStallTimeout,
		firstByte:     firstByteTimeout,
		headerTimeout: reopenHeaderTimeout,
		maxStalls:     maxConsecutiveStalls,
		maxResumes:    maxTotalResumes,
	}
}

func (t *resumableTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// the body that arrives before any resume needs a cancel of its own, so that a stalled read can
	// be abandoned without touching the caller's context -- cancelling that would end the whole
	// pull. reopen mints one per attempt; this is the equivalent for the first attempt. the request
	// stored below is the caller's, not this clone, so resumes are still derived from the caller's
	// context and finished() still reads the caller's cancellation rather than ours.
	ctx, cancel := context.WithCancel(req.Context())

	resp, err := t.base.RoundTrip(req.WithContext(ctx))
	if err != nil || !t.shouldResume(req, resp) {
		return releasing(resp, cancel), err
	}

	resp.Body = &resumableBody{
		body:          resp.Body,
		cancel:        cancel,
		req:           req,
		base:          t.base,
		total:         resp.ContentLength,
		minProgress:   t.minProgress,
		backoff:       t.backoff,
		stallTimeout:  t.stallTimeout,
		firstByte:     t.firstByte,
		headerTimeout: t.headerTimeout,
		maxStalls:     t.maxStalls,
		maxResumes:    t.maxResumes,
		done:          make(chan struct{}),
	}

	return resp, nil
}

// releasing hands back a response this transport is not wrapping, arranging for the context minted
// for it to be released once the body is closed. cancelling here instead would abort the very body
// being returned.
func releasing(resp *http.Response, cancel context.CancelFunc) *http.Response {
	if resp == nil || resp.Body == nil {
		cancel()

		return resp
	}

	resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}

	return resp
}

// cancelOnClose releases a request context when the body it governs is closed. a CancelFunc is safe
// to call more than once, so a repeated Close needs no guard of its own.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	defer c.cancel()

	return c.ReadCloser.Close()
}

// shouldResume reports whether a response body is worth wrapping: a plain GET, not itself a range
// request so that the offset arithmetic starts from zero, which returned a body of known length
// large enough to be a layer blob.
//
// the length must be known because it is the only way to tell a stream that ended early from one
// that ended properly. it also keeps transparently decompressed responses out, since net/http sets
// ContentLength to -1 for those. the coding must be identity for the same reason: the offsets are
// always over wire bytes, and an identity segment cannot be spliced onto a stream that is not.
func (t *resumableTransport) shouldResume(req *http.Request, resp *http.Response) bool {
	return resp != nil &&
		req.Method == http.MethodGet &&
		// Clone shares the body reader, so a reissued request would resend a consumed one
		(req.Body == nil || req.Body == http.NoBody) &&
		req.Header.Get("Range") == "" &&
		resp.Body != nil &&
		resp.StatusCode == http.StatusOK &&
		// the offsets are over wire bytes, so a stream that arrives in some coding cannot have an
		// identity segment spliced onto it. net/http only strips a coding it asked for itself, and
		// that sets ContentLength to -1, so an explicit one from the server survives to here
		identityEncoded(resp.Header.Get("Content-Encoding")) &&
		resp.ContentLength >= t.minSize
}

// resumableBody is the io.ReadCloser handed back in place of a large response body. it tracks how
// many bytes it has delivered so that a failed read can be continued with a Range request rather
// than restarting a transfer that may already be gigabytes along.
type resumableBody struct {
	req           *http.Request
	base          http.RoundTripper
	total         int64
	minProgress   int64
	backoff       time.Duration
	stallTimeout  time.Duration
	firstByte     time.Duration
	headerTimeout time.Duration
	maxStalls     int
	maxResumes    int

	// mu guards body, cancel and closed, which Close and the stall watchdog may touch from another
	// goroutine while Read is blocked or mid-resume. a caller closing a Response.Body to abort a read
	// is ordinary usage, so the wrapper has to honour it -- and it is why Close cancels as well as
	// closes, since on HTTP/1 a Close alone would queue behind the very read it means to interrupt.
	mu     sync.Mutex
	body   io.ReadCloser
	closed bool

	// cancel ends the reopen currently in flight, or the body it produced. without it Close cannot
	// interrupt a RoundTrip that has not yet returned, and a silent endpoint would hold the reader
	// for as long as the pull's context allows
	cancel context.CancelFunc

	// done is shut by Close, so a backoff in progress is abandoned rather than run to term
	done chan struct{}

	// the remainder belong to the reader and are not guarded: io.Reader admits a single reader
	offset   int64
	progress int64
	// delivered records whether the transfer has produced a byte, which selects between the
	// first-byte budget and the mid-body one. it is never reset: the generous budget is for a cold
	// start, and a transfer only starts cold once
	delivered bool
	stalls    int
	resumes   int
	// failed latches the error that ended the stream, so a caller reading past a failure gets that
	// error back rather than starting the download over
	failed error
}

func (b *resumableBody) Read(p []byte) (int, error) {
	if b.failed != nil {
		return 0, b.failed
	}

	// never hand back more than the response declared, as an unwrapped net/http body would not
	remaining := b.total - b.offset
	if remaining <= 0 {
		return 0, b.latch(io.EOF)
	}

	if int64(len(p)) > remaining {
		p = p[:remaining]
	}

	for {
		//nolint:closecheck // borrowed from the struct, which owns it and closes it in Close
		body, err := b.currentBody()
		if err != nil {
			return 0, err
		}

		n, readErr := b.readWithDeadline(body, p)
		b.record(n)

		if readErr == nil {
			return n, nil
		}
		if b.isClosed() {
			return n, b.latch(http.ErrBodyReadAfterClose)
		}
		if done, final := b.finished(); done {
			return n, b.latch(final)
		}
		if resumeErr := b.resume(shortRead(readErr)); resumeErr != nil {
			return n, b.latch(resumeErr)
		}
		if n > 0 {
			// hand back what this read managed; the next call continues on the new connection
			return n, nil
		}
	}
}

// readWithDeadline performs one read under a deadline on the server producing anything at all.
//
// the deadline is disarmed as soon as the read returns, so a body that is merely slow is never
// interrupted: every read that delivers bytes arms a fresh one, and a caller that stops reading is
// not on a clock at all.
func (b *resumableBody) readWithDeadline(body io.Reader, p []byte) (int, error) {
	if b.stallTimeout <= 0 {
		return body.Read(p)
	}

	timeout := b.stallTimeout
	if !b.delivered && b.firstByte > 0 {
		timeout = b.firstByte
	}

	// the cancel is captured here and handed to the watchdog rather than read from the struct when
	// it fires. a timer that fires in the instant a read returns runs on its own goroutine, and by
	// the time it wins the mutex the reader may already have resumed -- at which point the field
	// holds a healthy attempt's cancel, and re-reading it would kill that instead of this one
	b.mu.Lock()
	cancel := b.cancel
	b.mu.Unlock()

	watchdog := time.AfterFunc(timeout, func() { b.abandonStalledRead(cancel, timeout) })
	n, readErr := body.Read(p)

	// Stop reporting false means the watchdog has fired, or is firing, so the request under this
	// read is on its way out whatever the read itself returned. keep the bytes and call it a stall.
	//
	// a cancellation here is ours rather than the caller's, and left bare it would reach a caller as
	// "context canceled" -- a Ctrl-C nobody pressed, the same confusion reopen guards against for
	// its own timer. an io.EOF is substituted for the same reason the terminal errors render their
	// cause: a stall is not a clean end of stream and must not be mistakable for one
	if !watchdog.Stop() && stalled(readErr) {
		readErr = errReadStalled
	}

	return n, readErr
}

// stalled reports whether a read error is one the watchdog should relabel: the read ended with
// nothing to say, or ended because the watchdog cancelled it. anything else is the server's own
// error and is more informative than the label would be.
func stalled(readErr error) bool {
	return readErr == nil ||
		errors.Is(readErr, io.EOF) ||
		errors.Is(readErr, context.Canceled) ||
		errors.Is(readErr, context.DeadlineExceeded)
}

// abandonStalledRead ends the request under the read in flight, which is what a read blocked inside
// net/http responds to. closing the body is not enough: on HTTP/1 http.body.Read holds a mutex for
// the length of the read, so Close would simply queue behind the read it is meant to interrupt.
//
// nothing owned by the reader is touched here, offset included, because this runs on the timer's
// goroutine while Read is still in flight on another.
func (b *resumableBody) abandonStalledRead(cancel context.CancelFunc, timeout time.Duration) {
	if cancel == nil {
		return
	}

	log.WithFields("url", forLog(b.req.URL), "timeout", timeout).
		Debug("blob stream went quiet, abandoning the connection so the read can resume")

	cancel()
}

// shortRead renames the error of a stream that stopped before its declared length was delivered.
//
// net/http reports a body that simply ends as io.EOF, and the terminal failures below wrap their
// cause so that the sentinels this path raises stay classifiable. wrapping a bare io.EOF along with
// them would make errors.Is(err, io.EOF) true on a truncation, and a clean end of stream is exactly
// what a truncation must never be mistakable for: this repo's own file.IterateTar breaks on that
// test and returns nil, as do several go-containerregistry helpers, so a layer we failed to fetch
// would become a silently short SBOM -- the outcome this whole transport exists to prevent.
//
// it is called where the stream is known to be short, after finished() has ruled out a body that
// arrived in full.
func shortRead(readErr error) error {
	if errors.Is(readErr, io.EOF) {
		return io.ErrUnexpectedEOF
	}

	return readErr
}

// latch records the error that ended the stream so every subsequent Read returns it, rather than
// going round again on a body that is finished or dead.
func (b *resumableBody) latch(err error) error {
	b.failed = err

	return err
}

// record accounts for bytes just delivered, replenishing the stall budget once enough have arrived
// to count as real progress.
func (b *resumableBody) record(n int) {
	if n <= 0 {
		return
	}

	b.offset += int64(n)
	b.progress += int64(n)
	b.delivered = true

	if b.progress >= b.minProgress {
		b.stalls = 0
		b.progress = 0
	}
}

// finished reports whether the read is over, and with which error. a body that arrived in full is
// finished whatever the connection made of it afterwards -- over HTTP/2 a RST_STREAM following the
// final DATA frame surfaces as a stream error rather than io.EOF -- and otherwise a cancelled
// context ends the read as its own error, so a truncated stream is never mistaken for a clean end.
func (b *resumableBody) finished() (bool, error) {
	if b.offset >= b.total {
		return true, io.EOF
	}

	if ctxErr := b.req.Context().Err(); ctxErr != nil {
		return true, ctxErr
	}

	return false, nil
}

// resume closes the failed connection and reopens the request from the current offset, retrying
// until it succeeds or runs out of budget.
func (b *resumableBody) resume(cause error) error {
	b.closeCurrent()

	for {
		b.resumes++
		if b.resumes > b.maxResumes {
			return fmt.Errorf("gave up resuming %s at byte %d after %d attempts in total: %w",
				forLog(b.req.URL), b.offset, b.maxResumes, cause)
		}

		b.stalls++
		if b.stalls > b.maxStalls {
			return fmt.Errorf("gave up resuming %s at byte %d after %d attempts without progress: %w",
				forLog(b.req.URL), b.offset, b.maxStalls, cause)
		}

		// Info rather than Debug: a run of these is the only sign an operator has that a slow pull
		// is recovering rather than wedged, and there are at most maxTotalResumes of them
		log.WithFields("url", forLog(b.req.URL), "offset", b.offset, "error", cause).
			Info("blob stream dropped, resuming from the current offset")

		// the first attempt after a drop goes out at once; only repeated failures back off. sleep
		// is called either way, so cancellation and Close are noticed on every attempt
		var delay time.Duration
		if b.stalls > 1 {
			delay = time.Duration(b.stalls-1) * b.backoff
		}

		if err := b.sleep(delay); err != nil {
			return err
		}

		//nolint:closecheck // ownership passes to adopt, which either stores it or closes it
		body, err := b.reopen()
		if err != nil {
			if permanentResumeError(err) {
				return fmt.Errorf("cannot resume %s at byte %d after %w: %w",
					forLog(b.req.URL), b.offset, cause, err)
			}

			// the same renaming the read error gets, and for the same reason: http.Transport hands
			// back a bare io.EOF when a peer accepts the connection, reads the request and closes
			// without answering -- which is precisely what a drained CDN edge does, and is how this
			// path is reached at all
			cause = shortRead(err)

			continue
		}

		return b.adopt(body)
	}
}

// permanentResumeError reports whether retrying could possibly mend the error, so that a server
// which cannot resume fails at once rather than after the whole budget has been spent.
func permanentResumeError(err error) bool {
	return errors.Is(err, errNoRangeSupport) ||
		errors.Is(err, errObjectChanged) ||
		errors.Is(err, errCredentialsRejected) ||
		errors.Is(err, http.ErrBodyReadAfterClose)
}

// adopt installs a freshly opened body, unless the caller closed us while it was being opened.
func (b *resumableBody) adopt(body io.ReadCloser) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		closeQuietly(body)

		return http.ErrBodyReadAfterClose
	}

	b.body = body

	return nil
}

// sleep waits for d, or until the request's context is cancelled or the body closed, whichever
// comes first. the cancellation checks come before the zero-delay short-circuit so that a zero
// backoff, which is how the tests are configured, exercises the same behaviour as production.
func (b *resumableBody) sleep(d time.Duration) error {
	ctx := b.req.Context()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return http.ErrBodyReadAfterClose
	default:
	}

	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return http.ErrBodyReadAfterClose
	case <-timer.C:
		return nil
	}
}

// reopen issues the original request again for the bytes not yet delivered.
//
// the range is closed rather than open-ended. go-containerregistry's own registry -- which is what
// `crane registry serve` and ko run, and which a good many CI harnesses embed -- parses the header
// with Sscanf("bytes=%d-%d") and answers 416 to "bytes=N-", which would discard the layer on its
// first drop. every registry probed for this change answered a closed range correctly.
//
// identity is asked for because the stream so far is wire bytes: splicing a decoded segment onto it
// would be caught by a blob's digest but by nothing at all on a wrapped manifest.
func (b *resumableBody) reopen() (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(b.req.Context())
	if !b.adoptCancel(cancel) {
		cancel()

		return nil, http.ErrBodyReadAfterClose
	}

	req := b.req.Clone(ctx)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", b.offset, b.total-1))
	req.Header.Set("Accept-Encoding", "identity")

	// stopped once headers arrive, so the clock is on reaching the server rather than on the
	// transfer that follows. a timer that fires in the instant before Stop costs one attempt of the
	// budget, which is why this is not the only thing bounding the read
	var headers *time.Timer
	if b.headerTimeout > 0 {
		headers = time.AfterFunc(b.headerTimeout, cancel)
	}

	resp, err := b.base.RoundTrip(req)
	timedOut := headers != nil && !headers.Stop()

	if err != nil {
		if timedOut {
			// the cancellation was ours, not the caller's. left bare it surfaces as "context
			// canceled", which is indistinguishable from a Ctrl-C nobody pressed
			return nil, fmt.Errorf("no response headers within %s: %v", b.headerTimeout, err)
		}

		return nil, err
	}

	if timedOut {
		// the timer fired in the instant before Stop, so this response is already condemned: its
		// context is cancelled and the first read of it would fail. spending an attempt on that
		// body is worse than spending one here, where the reason is still known
		if resp.Body != nil {
			closeQuietly(resp.Body)
		}

		return nil, fmt.Errorf("no response headers within %s", b.headerTimeout)
	}

	return b.acceptable(resp)
}

// adoptCancel stores the cancel belonging to the reopen now in flight, ending any it replaces, and
// reports whether the caller is still wanted -- Close may have arrived while it was being opened.
func (b *resumableBody) adoptCancel(cancel context.CancelFunc) bool {
	b.mu.Lock()
	previous := b.cancel
	b.cancel = cancel
	closed := b.closed
	b.mu.Unlock()

	if previous != nil {
		previous()
	}

	return !closed
}

// acceptable decides whether a reissued response may be spliced onto the stream, closing it if not.
// a 3xx is refused permanently rather than followed: this transport sits below
// go-containerregistry's checkRedirectSSRF, so honouring a Location from here would reach a host
// that client never vetted. refusedStatus sorts the rest into what is worth another attempt.
func (b *resumableBody) acceptable(resp *http.Response) (io.ReadCloser, error) {
	if resp.Body == nil {
		return nil, fmt.Errorf("%w: the server returned no body", errNoRangeSupport)
	}

	reject := func(err error) (io.ReadCloser, error) {
		closeQuietly(resp.Body)

		return nil, err
	}

	if resp.StatusCode != http.StatusPartialContent {
		return reject(b.refusedStatus(resp))
	}

	// the coding check guards a body about to be spliced, so it belongs below the status one. run
	// first, it read a busy server that serves its error page compressed as one that cannot resume
	// at all, and that verdict is permanent
	if !identityEncoded(resp.Header.Get("Content-Encoding")) {
		return reject(fmt.Errorf("%w: the resumed segment came back encoded", errNoRangeSupport))
	}

	start, end, total, err := parseContentRange(resp.Header.Get("Content-Range"))
	if err != nil {
		return reject(fmt.Errorf("%w: %w", errNoRangeSupport, err))
	}

	if start != b.offset {
		return reject(fmt.Errorf("%w: server resumed at byte %d, expected %d",
			errNoRangeSupport, start, b.offset))
	}

	// RFC 9110 requires a 206 to give a last byte position, and either a complete length or "*". a
	// Content-Range carrying neither is malformed, and it is the one combination that switches off
	// both the span check below and the changed-object check after it at once, leaving the start
	// offset as the only thing between us and a body of the right length and the wrong bytes.
	// tolerating the leniencies separately is deliberate; tolerating both at once is not
	if end < 0 && total < 0 {
		return reject(fmt.Errorf("%w: Content-Range gives neither a last byte position nor a total",
			errNoRangeSupport))
	}

	if end >= 0 {
		if end < start {
			return reject(fmt.Errorf("%w: Content-Range ends at byte %d, before the byte %d it starts at",
				errNoRangeSupport, end, start))
		}

		if end > b.total-1 {
			return reject(fmt.Errorf("%w: Content-Range ends at byte %d, past the last byte of a %d byte object",
				errNoRangeSupport, end, b.total))
		}

		// a server that ignored the Range and began the object again, while still echoing back the
		// range it was asked for, contradicts itself here: the body it declares is longer than the
		// span it claims to be sending. it cannot catch a server whose lengths agree and whose bytes
		// are simply wrong -- only the digest above can do that -- but it is the shape a server with
		// no real range support actually takes, and catching it here costs one request instead of a
		// whole layer
		if span := end - start + 1; resp.ContentLength >= 0 && resp.ContentLength != span {
			return reject(fmt.Errorf("%w: Content-Range covers %d bytes but %d were sent",
				errNoRangeSupport, span, resp.ContentLength))
		}
	}

	if total >= 0 && total != b.total {
		return reject(fmt.Errorf("%w: the blob is now %d bytes, it was %d when the read began",
			errObjectChanged, total, b.total))
	}

	return resp.Body, nil
}

// refusedStatus classifies a reopen that did not come back as 206.
//
// the distinction matters more than it looks. lumping every status into "this server cannot
// resume" made a busy CDN indistinguishable from one that has never heard of Range, and since that
// verdict is permanent a single 503 discarded however many gigabytes had already arrived -- while
// a dropped TCP connection from the very same host was survivable. that asymmetry was not intended.
func (b *resumableBody) refusedStatus(resp *http.Response) error {
	switch {
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		// an expired bearer token, or a pre-signed CDN URL that outlived the transfer
		return fmt.Errorf("%w: got %d resuming at byte %d", errCredentialsRejected,
			resp.StatusCode, b.offset)

	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError:
		// the server's own Retry-After is deliberately not read: the stall backoff already spaces
		// repeated attempts, and honouring a hint of minutes would only lengthen a pull that is
		// already failing
		return fmt.Errorf("%w: got %d resuming at byte %d", errResumeUnavailable,
			resp.StatusCode, b.offset)

	default:
		// 416, a plain 200, a redirect: the server understood the request and will not serve it
		return fmt.Errorf("%w: expected HTTP 206 resuming at byte %d, got %d",
			errNoRangeSupport, b.offset, resp.StatusCode)
	}
}

// identityEncoded reports whether a Content-Encoding may be spliced onto the stream so far.
//
// "identity" is not a valid content-coding for a response, but a server that is echoing the
// Accept-Encoding it was sent will return it anyway, and reopen always asks for identity. treating
// that as an encoding would permanently refuse to resume from such a server.
func identityEncoded(encoding string) bool {
	encoding = strings.TrimSpace(encoding)

	return encoding == "" || strings.EqualFold(encoding, "identity")
}

// currentBody returns the body to read from, or an error if the caller has closed us.
func (b *resumableBody) currentBody() (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed || b.body == nil {
		return nil, http.ErrBodyReadAfterClose
	}

	return b.body, nil
}

func (b *resumableBody) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.closed
}

// closeCurrent drops the failed connection without marking the body closed to callers.
func (b *resumableBody) closeCurrent() {
	b.mu.Lock()
	body := b.body
	cancel := b.cancel
	b.body, b.cancel = nil, nil
	b.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if body != nil {
		closeQuietly(body)
	}
}

func (b *resumableBody) Close() error {
	b.mu.Lock()
	first := !b.closed
	b.closed = true
	body := b.body
	cancel := b.cancel
	b.body, b.cancel = nil, nil
	b.mu.Unlock()

	if first {
		// wakes a backoff in progress; guarded by first so a second Close cannot panic
		close(b.done)
	}

	// ends a reopen that has not yet returned, which closing the body cannot reach
	if cancel != nil {
		cancel()
	}

	if body == nil {
		return nil
	}

	return body.Close()
}

// parseContentRange pulls the first and last byte positions and the total length out of a
// Content-Range header, e.g. the 4096, 8191 and 16384 in "bytes 4096-8191/16384". a total given as
// "*" is unknown and is reported as -1, as is a last byte position that will not parse.
func parseContentRange(header string) (int64, int64, int64, error) {
	malformed := func() (int64, int64, int64, error) {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range %q", header)
	}

	unit, spec, ok := strings.Cut(strings.TrimSpace(header), " ")
	if !ok || !strings.EqualFold(unit, "bytes") {
		return malformed()
	}

	span, size, ok := strings.Cut(strings.TrimSpace(spec), "/")
	if !ok {
		return malformed()
	}

	first, last, ok := strings.Cut(span, "-")
	if !ok {
		return malformed()
	}

	start, err := strconv.ParseInt(strings.TrimSpace(first), 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range %q: %w", header, err)
	}

	// the last byte position is advisory here: it is cross-checked against Content-Length when both
	// are present, but a server that omits or mangles it is not refused on that account alone
	end, err := strconv.ParseInt(strings.TrimSpace(last), 10, 64)
	if err != nil {
		end = -1
	}

	if size = strings.TrimSpace(size); size == "*" {
		return start, end, -1, nil
	}

	total, err := strconv.ParseInt(size, 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("malformed Content-Range %q: %w", header, err)
	}

	return start, end, total, nil
}

// forLog renders a URL without its query string, which for a CDN-signed blob URL carries the
// access token.
func forLog(u *url.URL) string {
	return u.Host + u.Path
}

func closeQuietly(c io.Closer) {
	if err := c.Close(); err != nil {
		log.WithFields("error", err).Trace("unable to close dropped blob stream")
	}
}
