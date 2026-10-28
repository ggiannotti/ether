package usenet

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/braedonsaunders/ether/internal/config"
	"github.com/braedonsaunders/ether/internal/core"
	"github.com/braedonsaunders/ether/internal/usenet/nntptest"
	"github.com/braedonsaunders/ether/internal/yenc"
)

func pipelineReply(tb testing.TB, payload string) string {
	tb.Helper()
	b, err := yenc.EncodeBytes([]byte(payload), yenc.EncodeOptions{Name: "test.bin"})
	if err != nil {
		tb.Fatal(err)
	}
	// Encode includes CRLF but NNTP dot-stuffing is the transport's job.
	lines := strings.Split(string(b), "\r\n")
	for i, line := range lines {
		if strings.HasPrefix(line, ".") {
			lines[i] = "." + line
		}
	}
	return "222 0 <article@test>\r\n" + strings.Join(lines, "\r\n") + ".\r\n"
}

// The server refuses to reply until it has read window commands. This catches
// pseudo-pipelining that merely loops over synchronous BodyDecode calls.
func pipelineDial(tb testing.TB, window int, replies []string, observed chan<- []string) func(context.Context, string, string) (net.Conn, error) {
	return pipelineDialGate(tb, window, replies, observed, nil)
}

func pipelineDialGate(tb testing.TB, window int, replies []string, observed chan<- []string, gate <-chan struct{}) func(context.Context, string, string) (net.Conn, error) {
	tb.Helper()
	return func(ctx context.Context, _, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		done := make(chan struct{})
		tb.Cleanup(func() { client.Close(); server.Close(); <-done })
		go func() {
			defer close(done)
			defer server.Close()
			server.SetDeadline(time.Now().Add(3 * time.Second))
			io.WriteString(server, "200 test\r\n")
			r := bufio.NewReader(server)
			lines := make([]string, window)
			for i := range lines {
				line, err := r.ReadString('\n')
				if err != nil {
					return
				}
				lines[i] = strings.TrimSpace(line)
			}
			if observed != nil {
				observed <- lines
			}
			if gate != nil {
				select {
				case <-gate:
				case <-time.After(3 * time.Second):
					return
				}
			}
			for _, reply := range replies {
				if _, err := io.WriteString(server, reply); err != nil {
					return
				}
			}
			// A following STAT proves the previous body framing was completely drained.
			if line, err := r.ReadString('\n'); err == nil && strings.HasPrefix(line, "STAT ") {
				io.WriteString(server, "223 yes\r\n")
			}
		}()
		return client, nil
	}
}

func TestBodyPipelineOrderedIsolatedResponses(t *testing.T) {
	replies := []string{pipelineReply(t, "first"), "430 absent\r\n", "222 0 <bad@test>\r\nnot yenc\r\n.\r\n", pipelineReply(t, "last")}
	wire := make(chan []string, 1)
	c, err := Dial(context.Background(), DialOptions{Host: "test", DialContext: pipelineDial(t, 4, replies, wire)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ids := []string{"first@test", "missing@test", "bad@test", "last@test"}
	var parts []*yenc.Part
	var errs []error
	c.bodyPipeline(context.Background(), ids, nil, func(i int, p *yenc.Part, e error) {
		if i != len(errs) {
			t.Fatalf("out of order %d", i)
		}
		parts = append(parts, p)
		errs = append(errs, e)
	})
	commands := <-wire
	for i, id := range ids {
		if commands[i] != "BODY <"+id+">" {
			t.Fatalf("command %q", commands[i])
		}
	}
	if errs[0] != nil || !errors.Is(errs[1], ErrNoSuchArticle) || !errors.Is(errs[2], ErrCorrupt) || errs[3] != nil {
		t.Fatalf("errors %v", errs)
	}
	if string(parts[0].Data) != "first" || string(parts[3].Data) != "last" {
		t.Fatal("responses crossed")
	}
	if ok, err := c.Stat(context.Background(), "after@test"); !ok || err != nil {
		t.Fatalf("reuse %v %v", ok, err)
	}
}

func TestBodyPipelineIntegratedBoundAndWire(t *testing.T) {
	wire := make(chan []string, 1)
	reply := pipelineReply(t, "payload")
	opts := Options{MinIdle: -1, KeepAlive: -1, BodyPipelineDepth: 4, DialContext: pipelineDial(t, 4, []string{reply, reply, reply, reply}, wire), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	opts.withDefaults()
	p := newPool("test", config.UsenetProvider{Host: "test", Connections: 1}, &opts, opts.Logger, -1)
	defer p.close()
	c, err := p.get(context.Background(), core.PriorityLive)
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func(i int) {
			part, err := p.body(context.Background(), fmt.Sprintf("%d@test", i), nil, core.PriorityLive)
			if err == nil && string(part.Data) != "payload" {
				err = errors.New("wrong payload")
			}
			results <- err
		}(i)
	}
	waitPipeline(t, func() bool { p.bodyMu.Lock(); defer p.bodyMu.Unlock(); return len(p.bodyQueue[core.PriorityLive]) == 3 })
	p.put(c, core.PriorityLive)
	for i := 0; i < 4; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if got := <-wire; len(got) != 4 {
		t.Fatal(got)
	}
	if st := p.stats(); st.Dials != 1 || st.BODYPipelined != 4 || st.Articles != 4 {
		t.Fatalf("stats %+v", st)
	}
}

// A worker that has dequeued a flight must not pipeline queued flights that
// a spawned-but-idle sibling worker could serve in parallel on a free socket.
// Only when no socket is free for the sibling does pipelining win.
func TestBodyWorkerLeavesQueuedFlightsForPendingSiblings(t *testing.T) {
	for _, tc := range []struct {
		name          string
		freeSlot      bool
		wantBatches   int64
		wantPipelined int64
	}{
		{name: "free slot", freeSlot: true, wantBatches: 2, wantPipelined: 0},
		{name: "no free slot", freeSlot: false, wantBatches: 1, wantPipelined: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := nntptest.Start(t, nntptest.Options{})
			addSeg(t, s, "leader@test", []byte("leader"))
			addSeg(t, s, "queued@test", []byte("queued"))
			f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 3)}, Options{KeepAlive: -1, BodyPipelineDepth: 4})
			p := f.pools[0]
			prio := core.PriorityLive

			// Build the flights by hand so the worker starts with both
			// already queued: no request goroutine can race the batching
			// decision (e.g. by returning a held socket a moment early).
			newFlight := func(id string) *bodyFlight {
				if !p.bodyAdmission.TryAcquire(prio) {
					t.Fatal("body admission should have room")
				}
				ctx, cancel := context.WithCancel(context.Background())
				return &bodyFlight{key: bodyKey{id: id, prio: prio}, ctx: ctx, cancel: cancel, done: make(chan struct{}), waiters: 1}
			}
			flights := []*bodyFlight{newFlight("<leader@test>"), newFlight("<queued@test>")}
			p.bodyMu.Lock()
			for _, fl := range flights {
				p.bodyFlights[fl.key] = fl
				p.bodyQueue[prio] = append(p.bodyQueue[prio], fl)
			}
			// One worker runs below; the second counts as a pending
			// sibling that is spawned but never started.
			p.bodyWorkers[prio] = 2
			p.bodyMu.Unlock()

			// With no free slot the pending sibling cannot take the
			// queued flight in parallel, so the running worker pipelines
			// it; with a free slot it must leave it for the sibling.
			var held []*Conn
			if !tc.freeSlot {
				for range 2 {
					c, err := p.get(context.Background(), prio)
					if err != nil {
						t.Fatal(err)
					}
					held = append(held, c)
				}
			}

			p.bodyWG.Add(1)
			go p.bodyWorker(prio)
			for i, fl := range flights {
				<-fl.done
				if fl.err != nil {
					t.Fatal(fl.err)
				}
				if want := []string{"leader", "queued"}[i]; string(fl.part.Data) != want {
					t.Fatalf("flight %d payload %q, want %q", i, fl.part.Data, want)
				}
			}
			p.bodyWG.Wait()
			// The pending sibling never ran; drop its worker count.
			p.bodyMu.Lock()
			p.bodyWorkers[prio]--
			p.bodyMu.Unlock()
			for _, c := range held {
				p.put(c, prio)
			}
			if st := p.stats(); st.BODYBatches != tc.wantBatches || st.BODYPipelined != tc.wantPipelined {
				t.Fatalf("batches=%d pipelined=%d, want %d/%d (%+v)", st.BODYBatches, st.BODYPipelined, tc.wantBatches, tc.wantPipelined, st)
			}
			p.bodyMu.Lock()
			claimed := p.bodyClaimed
			p.bodyMu.Unlock()
			if claimed != [numPriorities]int{} {
				t.Fatalf("bodyClaimed = %v, want [0 0 0]", claimed)
			}
		})
	}
}

func waitPipeline(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for pipeline state")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBodySingleflightIndependentCancellation(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	addSeg(t, s, "shared@test", []byte("payload"))
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd == "BODY" {
			once.Do(func() { close(entered) })
			<-release
		}
		return nntptest.Fault{}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 2)}, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { _, err := f.Body(ctx, "shared@test", nil, core.PriorityLive); first <- err }()
	<-entered
	go func() {
		b, err := f.Body(context.Background(), "shared@test", nil, core.PriorityLive)
		if err == nil && string(b) != "payload" {
			err = errors.New("wrong payload")
		}
		second <- err
	}()
	p := f.pools[0]
	waitPipeline(t, func() bool {
		p.bodyMu.Lock()
		defer p.bodyMu.Unlock()
		for _, flight := range p.bodyFlights {
			if flight.waiters == 2 {
				return true
			}
		}
		return false
	})
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if got := s.BodyRequests("shared@test"); got != 1 {
		t.Fatalf("downloaded duplicate %d times", got)
	}
}

func TestBodyPipelineCloseInterruptsAndJoins(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	addSeg(t, s, "slow@test", []byte("payload"))
	s.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd == "BODY" {
			return nntptest.Fault{BodyDelay: time.Second}
		}
		return nntptest.Fault{}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 1)}, Options{})
	p := f.pools[0]
	done := make(chan error, 1)
	go func() { _, err := f.Body(context.Background(), "slow@test", nil, core.PriorityLive); done <- err }()
	waitPipeline(t, func() bool { return s.BodyRequests("slow@test") == 1 })
	start := time.Now()
	f.Close()
	if time.Since(start) > 250*time.Millisecond {
		t.Fatal("close waited for stalled body")
	}
	if err := <-done; err == nil {
		t.Fatal("closed fetch succeeded")
	}
	if st := p.sem.stats(); st.InUse != 0 {
		t.Fatalf("leaked slots %+v", st)
	}
	p.bodyMu.Lock()
	defer p.bodyMu.Unlock()
	if len(p.bodyFlights) != 0 || p.bodyWorkers[0] != 0 {
		t.Fatal("shutdown retained workers/flights")
	}
}

func TestBodyPipelineTransportFailurePoisonsOnlyRemaining(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	for _, id := range []string{"first@test", "broken@test", "last@test"} {
		addSeg(t, s, id, bytes.Repeat([]byte("abc"), 100))
	}
	s.SetFault(func(cmd, id string) nntptest.Fault {
		return nntptest.Fault{Truncate: cmd == "BODY" && id == "broken@test"}
	})
	c := dialServer(t, s)
	var errs []error
	c.bodyPipeline(context.Background(), []string{"first@test", "broken@test", "last@test"}, nil, func(i int, p *yenc.Part, e error) { errs = append(errs, e) })
	if len(errs) != 3 || errs[0] != nil || errs[1] == nil || errs[2] == nil || !c.Broken() {
		t.Fatalf("errors %v broken %v", errs, c.Broken())
	}
}

func TestBreakerSingleRecoveryCommand(t *testing.T) {
	now := time.Unix(1, 0)
	b := newBreaker(time.Second, time.Minute, 1, func() time.Time { return now })
	b.connFailure(io.EOF)
	if _, err := b.admit(); !errors.Is(err, ErrProviderBackoff) {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if probe, err := b.admit(); !probe || err != nil {
		t.Fatalf("probe %v %v", probe, err)
	}
	for i := 0; i < 20; i++ {
		if _, err := b.admit(); !errors.Is(err, ErrProviderBackoff) {
			t.Fatal("recovery herd admitted")
		}
	}
	b.releaseProbe() // A canceled request must not wedge half-open recovery.
	if probe, err := b.admit(); !probe || err != nil {
		t.Fatalf("retry %v %v", probe, err)
	}
	b.success()
	b.releaseProbe()
	if probe, err := b.admit(); probe || err != nil {
		t.Fatalf("healthy %v %v", probe, err)
	}
}

func TestBodyPipelineCanceledMemberDoesNotAbortPeers(t *testing.T) {
	wire := make(chan []string, 1)
	gate := make(chan struct{})
	reply := pipelineReply(t, "payload")
	o := Options{MinIdle: -1, KeepAlive: -1, BodyPipelineDepth: 4, DialContext: pipelineDialGate(t, 3, []string{reply, reply, reply}, wire, gate), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	o.withDefaults()
	p := newPool("test", config.UsenetProvider{Host: "test", Connections: 1}, &o, o.Logger, -1)
	defer p.close()
	c, err := p.get(context.Background(), core.PriorityLive)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	defer cancel()
	gone := make(chan error, 1)
	peers := make(chan error, 2)
	go func() { _, err := p.body(canceled, "cancel@test", nil, core.PriorityLive); gone <- err }()
	for i := 0; i < 2; i++ {
		go func(i int) {
			part, err := p.body(context.Background(), fmt.Sprintf("peer%d@test", i), nil, core.PriorityLive)
			if err == nil && string(part.Data) != "payload" {
				err = errors.New("wrong peer body")
			}
			peers <- err
		}(i)
	}
	waitPipeline(t, func() bool { p.bodyMu.Lock(); defer p.bodyMu.Unlock(); return len(p.bodyQueue[0]) == 2 })
	p.put(c, core.PriorityLive)
	<-wire // All three BODY commands are irrevocably on the socket.
	cancel()
	select {
	case err := <-gone:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled member waited for its body")
	}
	close(gate)
	for i := 0; i < 2; i++ {
		if err := <-peers; err != nil {
			t.Fatal(err)
		}
	}
	waitPipeline(t, func() bool { return p.sem.stats().InUse == 0 })
	// If the abandoned member is last, completing both peers can cancel
	// the remaining response instead of wasting bandwidth draining it.
	if st := p.stats(); st.Errors != 0 || st.Articles < 2 || st.Articles > 3 {
		t.Fatalf("healthy peers poisoned %+v", st)
	}
}

func TestBodyPipelineShutdownIncludesReloadedPool(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	addSeg(t, s, "slow@test", []byte("payload"))
	s.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd == "BODY" {
			return nntptest.Fault{BodyDelay: time.Second}
		}
		return nntptest.Fault{}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 1)}, Options{})
	old := f.pools[0]
	done := make(chan error, 1)
	go func() { _, err := f.Body(context.Background(), "slow@test", nil, core.PriorityLive); done <- err }()
	waitPipeline(t, func() bool { return s.BodyRequests("slow@test") == 1 })
	f.Update(nil) // Keep the accepted old window graceful until explicit shutdown.
	start := time.Now()
	f.Close()
	if time.Since(start) > 250*time.Millisecond {
		t.Fatal("retired window not interrupted")
	}
	if err := <-done; err == nil {
		t.Fatal("shutdown completed stalled old article")
	}
	if old.hasBodyWork() {
		t.Fatal("retired pool retained BODY workers")
	}
}

func TestBodyPipelineTruncatedPredecessorsDoNotExhaustTailRetries(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	const count = 96
	for i := 0; i < count; i++ {
		addSeg(t, s, fmt.Sprintf("part%d@test", i), bytes.Repeat([]byte{byte(i)}, 4096))
	}
	var mu sync.Mutex
	seen := map[string]bool{}
	s.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd != "BODY" {
			return nntptest.Fault{}
		}
		var index int
		fmt.Sscanf(id, "part%d@test", &index)
		mu.Lock()
		first := !seen[id]
		seen[id] = true
		mu.Unlock()
		return nntptest.Fault{Truncate: first && index%7 == 0}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 4)}, Options{BodyPipelineDepth: 4})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var next atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for w := 0; w < 16; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				index := int(next.Add(1) - 1)
				if index >= count {
					return
				}
				data, err := f.Body(ctx, fmt.Sprintf("part%d@test", index), nil, core.PriorityLive)
				if err == nil && !bytes.Equal(data, bytes.Repeat([]byte{byte(index)}, 4096)) {
					err = errors.New("wrong article bytes")
				}
				if err != nil {
					errs <- fmt.Errorf("part%d: %w", index, err)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestBodyRecoveryPersistentTruncationIsBounded(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	addSeg(t, s, "bad@test", bytes.Repeat([]byte("bad"), 4096))
	s.SetFault(func(cmd, id string) nntptest.Fault { return nntptest.Fault{Truncate: cmd == "BODY"} })
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 4)}, Options{FailThreshold: 1, BackoffMin: 20 * time.Millisecond, BackoffMax: 20 * time.Millisecond})
	started := time.Now()
	_, err := f.Body(context.Background(), "bad@test", nil, core.PriorityLive)
	if err == nil || errors.Is(err, core.ErrArticleMissing) {
		t.Fatalf("wrong error %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2500*time.Millisecond {
		t.Fatalf("unbounded recovery %s", elapsed)
	}
	if requests := s.BodyRequests("bad@test"); requests < 2 || requests > 9 {
		t.Fatalf("recovery request bound: %d", requests)
	}
}

func TestBodyRecoveryCallerCancellation(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	addSeg(t, s, "good@test", []byte("good"))
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 1)}, Options{FailThreshold: 1, BackoffMin: 10 * time.Second, BackoffMax: 10 * time.Second})
	f.pools[0].brk.connFailure(io.EOF)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := f.Body(ctx, "good@test", nil, core.PriorityLive)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 300*time.Millisecond {
		t.Fatalf("cancellation %v after %s", err, time.Since(start))
	}
	if requests := s.BodyRequests("good@test"); requests != 0 {
		t.Fatalf("cooldown ignored: %d commands", requests)
	}
}

func TestReadaheadPipelineReservesUrgentLiveSocket(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	addSeg(t, s, "urgent@test", []byte("urgent"))
	for i := 0; i < 12; i++ {
		addSeg(t, s, fmt.Sprintf("ahead%d@test", i), []byte("ahead"))
	}
	gate := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	entered := make(chan struct{}, 12)
	s.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd == "BODY" && strings.HasPrefix(id, "ahead") {
			entered <- struct{}{}
			<-gate
		}
		return nntptest.Fault{}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 4)}, Options{BodyPipelineDepth: 4})
	defer release()
	p := f.pools[0]
	var held []*Conn
	for i := 0; i < 4; i++ {
		c, err := p.get(context.Background(), core.PriorityLive)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	done := make(chan error, 12)
	for i := 0; i < 12; i++ {
		go func(i int) {
			_, err := f.Body(context.Background(), fmt.Sprintf("ahead%d@test", i), nil, core.PriorityReadahead)
			done <- err
		}(i)
	}
	waitPipeline(t, func() bool {
		p.bodyMu.Lock()
		defer p.bodyMu.Unlock()
		return len(p.bodyQueue[core.PriorityReadahead]) == 8
	})
	for _, c := range held {
		p.put(c, core.PriorityLive)
	}
	for i := 0; i < 3; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("readahead did not saturate allowed sockets")
		}
	}
	if st := p.stats(); st.ActiveConnections != 3 || st.BODYPipelined != 6 {
		t.Fatalf("expected3 two-deep readahead windows: %+v", st)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	data, err := f.Body(ctx, "urgent@test", nil, core.PriorityLive)
	if err != nil || string(data) != "urgent" {
		t.Fatalf("urgent live blocked behind readahead: %q %v", data, err)
	}
	// Three delayed windows are still blocked: live did not wait for any of them.
	if st := p.stats(); st.ActiveConnections != 3 {
		t.Fatalf("readahead unexpectedly released %+v", st)
	}
	release()
	for i := 0; i < 12; i++ {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
}

func TestMidBodyDropKeepsHealthyIdleSiblings(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	addSeg(t, s, "part@test", bytes.Repeat([]byte("data"), 4096))
	var first atomic.Bool
	s.SetFault(func(cmd, id string) nntptest.Fault {
		return nntptest.Fault{Truncate: cmd == "BODY" && first.CompareAndSwap(false, true)}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 4)}, Options{})
	if err := f.Warm(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Body(context.Background(), "part@test", nil, core.PriorityLive); err != nil {
		t.Fatal(err)
	}
	if st := f.Stats()[0]; st.Dials != 4 || st.LiveConnections != 3 {
		t.Fatalf("mid-body failure discarded healthy idle sockets: %+v", st)
	}
}

func TestLiveSharesTransmittedSpeculativeBody(t *testing.T) {
	for _, priority := range []core.Priority{core.PriorityReadahead, core.PriorityPrefetch} {
		t.Run(fmt.Sprint(priority), func(t *testing.T) {
			s := nntptest.Start(t, nntptest.Options{})
			payload := bytes.Repeat([]byte("payload"), 4096)
			addSeg(t, s, "shared@test", payload)
			entered, gate := make(chan struct{}, 1), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(gate) }) }
			defer release()
			s.SetFault(func(cmd, id string) nntptest.Fault {
				if cmd == "BODY" {
					entered <- struct{}{}
					<-gate
				}
				return nntptest.Fault{}
			})
			f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 4)}, Options{})
			p := f.pools[0]
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			background := make(chan error, 1)
			live := make(chan error, 1)
			go func() { _, err := f.Body(ctx, "shared@test", nil, priority); background <- err }()
			<-entered
			waitPipeline(t, func() bool {
				p.bodyMu.Lock()
				defer p.bodyMu.Unlock()
				fl := p.bodyFlights[bodyKey{id: "<shared@test>", prio: priority}]
				return fl != nil && fl.started
			})
			go func() {
				data, err := f.Body(context.Background(), "shared@test", nil, core.PriorityLive)
				if err == nil && !bytes.Equal(data, payload) {
					err = errors.New("wrong live payload")
				}
				live <- err
			}()
			waitPipeline(t, func() bool {
				p.bodyMu.Lock()
				defer p.bodyMu.Unlock()
				for _, fl := range p.bodyFlights {
					if fl.liveWaiters == 1 && fl.waiters == 2 {
						return true
					}
				}
				return false
			})
			cancel()
			if err := <-background; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			release()
			if err := <-live; err != nil {
				t.Fatal(err)
			}
			if n := s.BodyRequests("shared@test"); n != 1 {
				t.Fatalf("active hedge downloaded %d copies", n)
			}
			// Completion publishes the BODY before releasing worker admission.
			// Observe that release rather than racing the worker's cleanup.
			waitPipeline(t, func() bool { return p.bodyAdmission.stats().InUse == 0 })
		})
	}
}

func TestLiveBypassesQueuedSpeculativeBody(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	addSeg(t, s, "queued@test", []byte("body"))
	entered, gate := make(chan struct{}, 2), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	s.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd == "BODY" {
			entered <- struct{}{}
			<-gate
		}
		return nntptest.Fault{}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 1)}, Options{})
	p := f.pools[0]
	held, err := p.get(context.Background(), core.PriorityLive)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	background := make(chan error, 1)
	live := make(chan error, 1)
	go func() { _, err := f.Body(ctx, "queued@test", nil, core.PriorityReadahead); background <- err }()
	waitQueued(t, p.sem, core.PriorityReadahead, 1)
	go func() { _, err := f.Body(context.Background(), "queued@test", nil, core.PriorityLive); live <- err }()
	waitQueued(t, p.sem, core.PriorityLive, 1)
	p.put(held, core.PriorityLive)
	<-entered
	waitPipeline(t, func() bool {
		p.bodyMu.Lock()
		defer p.bodyMu.Unlock()
		fl := p.bodyFlights[bodyKey{id: "<queued@test>", prio: core.PriorityLive}]
		return fl != nil && fl.started
	})
	p.bodyMu.Lock()
	speculative := p.bodyFlights[bodyKey{id: "<queued@test>", prio: core.PriorityReadahead}]
	started := speculative != nil && speculative.started
	p.bodyMu.Unlock()
	if started {
		t.Fatal("queued speculative flight jumped ahead of live")
	}
	cancel()
	if err := <-background; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	release()
	if err := <-live; err != nil {
		t.Fatal(err)
	}
	if n := s.BodyRequests("queued@test"); n != 1 {
		t.Fatalf("canceled queued copy reached server %d times", n)
	}
}

func TestCrossPriorityJoinedPayloadsDoNotAlias(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	addSeg(t, s, "shared@test", []byte("payload"))
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	s.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd == "BODY" {
			<-gate
		}
		return nntptest.Fault{}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 2)}, Options{})
	data := make(chan []byte, 2)
	errs := make(chan error, 2)
	go func() {
		b, e := f.Body(context.Background(), "shared@test", nil, core.PriorityReadahead)
		data <- b
		errs <- e
	}()
	p := f.pools[0]
	waitPipeline(t, func() bool {
		p.bodyMu.Lock()
		defer p.bodyMu.Unlock()
		for _, fl := range p.bodyFlights {
			if fl.started {
				return true
			}
		}
		return false
	})
	go func() {
		b, e := f.Body(context.Background(), "shared@test", nil, core.PriorityLive)
		data <- b
		errs <- e
	}()
	waitPipeline(t, func() bool {
		p.bodyMu.Lock()
		defer p.bodyMu.Unlock()
		for _, fl := range p.bodyFlights {
			if fl.liveWaiters == 1 {
				return true
			}
		}
		return false
	})
	release()
	a, b := <-data, <-data
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	a[0] = 'X'
	if string(b) != "payload" {
		t.Fatal("different callers share mutable article bytes")
	}
}

func TestLiveJoinedSpeculativeTailReplaysAtLivePriority(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	for _, id := range []string{"first@test", "other@test", "tail@test"} {
		addSeg(t, s, id, []byte(id))
	}
	firstEntered, otherEntered := make(chan struct{}, 1), make(chan struct{}, 1)
	firstGate, otherGate := make(chan struct{}), make(chan struct{})
	var firstRelease, otherRelease sync.Once
	releaseFirst := func() { firstRelease.Do(func() { close(firstGate) }) }
	defer releaseFirst()
	releaseOther := func() { otherRelease.Do(func() { close(otherGate) }) }
	defer releaseOther()
	var firstSeen atomic.Bool
	s.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd != "BODY" {
			return nntptest.Fault{}
		}
		if id == "first@test" && firstSeen.CompareAndSwap(false, true) {
			firstEntered <- struct{}{}
			<-firstGate
			return nntptest.Fault{Truncate: true}
		}
		if id == "other@test" {
			otherEntered <- struct{}{}
			<-otherGate
		}
		return nntptest.Fault{}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 2)}, Options{BodyPipelineDepth: 4})
	p := f.pools[0]
	held := make([]*Conn, 2)
	for i := range held {
		var err error
		held[i], err = p.get(context.Background(), core.PriorityLive)
		if err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 3)
	request := func(id string) {
		go func() { _, err := f.Body(context.Background(), id, nil, core.PriorityReadahead); done <- err }()
	}
	request("first@test")
	waitQueued(t, p.sem, core.PriorityReadahead, 1)
	request("other@test")
	waitQueued(t, p.sem, core.PriorityReadahead, 2)
	request("tail@test")
	waitPipeline(t, func() bool {
		p.bodyMu.Lock()
		defer p.bodyMu.Unlock()
		return len(p.bodyQueue[core.PriorityReadahead]) == 1
	})
	p.put(held[0], core.PriorityLive)
	<-firstEntered
	waitPipeline(t, func() bool {
		p.bodyMu.Lock()
		defer p.bodyMu.Unlock()
		fl := p.bodyFlights[bodyKey{id: "<tail@test>", prio: core.PriorityReadahead}]
		return fl != nil && fl.started
	})
	live := make(chan error, 1)
	go func() {
		data, err := f.Body(context.Background(), "tail@test", nil, core.PriorityLive)
		if err == nil && string(data) != "tail@test" {
			err = errors.New("wrong tail")
		}
		live <- err
	}()
	waitPipeline(t, func() bool {
		p.bodyMu.Lock()
		defer p.bodyMu.Unlock()
		fl := p.bodyFlights[bodyKey{id: "<tail@test>", prio: core.PriorityReadahead}]
		return fl != nil && fl.liveWaiters == 1
	})
	releaseFirst() // Abort the original two-deep speculative window.
	<-otherEntered // Another speculative socket stays blocked throughout replay.
	waitQueued(t, p.sem, core.PriorityLive, 1)
	p.put(held[1], core.PriorityLive)
	select {
	case err := <-live:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("live tail demoted behind speculative retry")
	}
	releaseOther()
	for i := 0; i < 3; i++ {
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
	if n := s.BodyRequests("tail@test"); n != 1 {
		t.Fatalf("tail decoded %d times", n)
	}
}

func TestLiveDoesNotJoinSpeculativeGroupNegotiation(t *testing.T) {
	s := nntptest.Start(t, nntptest.Options{})
	addSeg(t, s, "group@test", []byte("payload"))
	entered, gate := make(chan struct{}, 1), make(chan struct{})
	var first atomic.Bool
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	defer release()
	s.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd == "GROUP" && first.CompareAndSwap(false, true) {
			entered <- struct{}{}
			<-gate
		}
		return nntptest.Fault{}
	})
	f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 2)}, Options{UseGroup: true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	background := make(chan error, 1)
	go func() {
		_, err := f.Body(ctx, "group@test", []string{"alt.binaries.test"}, core.PriorityReadahead)
		background <- err
	}()
	<-entered
	live, cancelLive := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancelLive()
	data, err := f.Body(live, "group@test", []string{"alt.binaries.test"}, core.PriorityLive)
	if err != nil || string(data) != "payload" {
		t.Fatalf("live inherited speculative GROUP wait: %q %v", data, err)
	}
	cancel()
	release()
	if err := <-background; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// Direct pool calls isolate the shared transfer's bounds from Fetcher retries.
func TestActiveJoinedStallHonorsDeadlinesAndCancellation(t *testing.T) {
	for _, mode := range []string{"idle", "command", "caller"} {
		t.Run(mode, func(t *testing.T) {
			s := nntptest.Start(t, nntptest.Options{})
			addSeg(t, s, "stalled@test", []byte("payload"))
			s.SetFault(func(cmd, id string) nntptest.Fault {
				if cmd == "BODY" {
					return nntptest.Fault{BodyDelay: 5 * time.Second}
				}
				return nntptest.Fault{}
			})
			o := Options{IdleReadTimeout: 200 * time.Millisecond, CommandTimeout: 3 * time.Second}
			if mode == "command" {
				o.IdleReadTimeout = -1
				o.CommandTimeout = 200 * time.Millisecond
			}
			f := newFetcher(t, []config.UsenetProvider{s.Provider("p", 2)}, o)
			p := f.pools[0]
			background, live := make(chan error, 1), make(chan error, 1)
			started := time.Now()
			go func() {
				_, err := p.body(context.Background(), "<stalled@test>", nil, core.PriorityReadahead)
				background <- err
			}()
			key := bodyKey{id: "<stalled@test>", prio: core.PriorityReadahead}
			waitPipeline(t, func() bool {
				p.bodyMu.Lock()
				defer p.bodyMu.Unlock()
				fl := p.bodyFlights[key]
				return fl != nil && fl.started && s.BodyRequests("stalled@test") == 1
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				_, err := p.body(ctx, "<stalled@test>", nil, core.PriorityLive)
				live <- err
			}()
			waitPipeline(t, func() bool {
				p.bodyMu.Lock()
				defer p.bodyMu.Unlock()
				fl := p.bodyFlights[key]
				return fl != nil && fl.liveWaiters == 1 && fl.waiters == 2
			})
			if mode == "caller" {
				cancel()
				select {
				case err := <-live:
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("joined caller cancellation: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("joined caller ignored cancellation")
				}
				p.bodyMu.Lock()
				fl := p.bodyFlights[key]
				retained := fl != nil && fl.waiters == 1 && fl.liveWaiters == 0 && fl.ctx.Err() == nil
				p.bodyMu.Unlock()
				if !retained {
					t.Fatal("canceling live waiter canceled the remaining speculative caller")
				}
			} else {
				select {
				case err := <-live:
					if !isTimeoutError(err) {
						t.Fatalf("joined caller should inherit network timeout: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("joined caller exceeded configured network bound")
				}
			}
			select {
			case err := <-background:
				if !isTimeoutError(err) {
					t.Fatalf("original caller should hit network timeout: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("shared stalled transfer ignored network deadlines")
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("network bound exceeded: %s", elapsed)
			}
			if n := s.BodyRequests("stalled@test"); n != 1 {
				t.Fatalf("active join sent %d BODY commands", n)
			}
			waitPipeline(t, func() bool { return p.bodyAdmission.stats().InUse == 0 && p.sem.stats().InUse == 0 })
			waitServerActive(t, s, 0)
		})
	}
}

func TestActiveJoinedStallBackfillsBothCallers(t *testing.T) {
	primary := nntptest.Start(t, nntptest.Options{})
	backup := nntptest.Start(t, nntptest.Options{})
	want := []byte("complete healthy backup payload")
	addSeg(t, primary, "stalled@test", want)
	addSeg(t, backup, "stalled@test", want)
	primary.SetFault(func(cmd, id string) nntptest.Fault {
		if cmd == "BODY" {
			return nntptest.Fault{BodyDelay: 5 * time.Second}
		}
		return nntptest.Fault{}
	})
	b := backup.Provider("backup", 2)
	b.Backfill = true
	f := newFetcher(t, []config.UsenetProvider{primary.Provider("primary", 2), b}, Options{
		IdleReadTimeout: 200 * time.Millisecond, CommandTimeout: 3 * time.Second,
	})
	p := f.pools[0]
	done := make(chan error, 2)
	request := func(prio core.Priority) {
		go func() {
			got, err := f.Body(context.Background(), "stalled@test", nil, prio)
			if err == nil && !bytes.Equal(got, want) {
				err = errors.New("backup payload mismatch")
			}
			done <- err
		}()
	}
	started := time.Now()
	request(core.PriorityReadahead)
	key := bodyKey{id: "<stalled@test>", prio: core.PriorityReadahead}
	waitPipeline(t, func() bool {
		p.bodyMu.Lock()
		defer p.bodyMu.Unlock()
		fl := p.bodyFlights[key]
		return fl != nil && fl.started
	})
	request(core.PriorityLive)
	waitPipeline(t, func() bool {
		p.bodyMu.Lock()
		defer p.bodyMu.Unlock()
		fl := p.bodyFlights[key]
		return fl != nil && fl.liveWaiters == 1
	})
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("shared primary stall prevented backup recovery")
		}
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("backup recovery exceeded idle deadline allowance: %s", elapsed)
	}
	if n := primary.BodyRequests("stalled@test"); n != 1 {
		t.Fatalf("shared stalled primary attempted %d times", n)
	}
	if n := backup.BodyRequests("stalled@test"); n < 1 || n > 2 {
		t.Fatalf("backup requests: %d", n)
	}
	waitServerActive(t, primary, 0)
}
