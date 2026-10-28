package usenet

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Provider states reported in statistics.
const (
	StateOK       = "ok"
	StateBackoff  = "backoff"     // connection failures; retrying after a delay
	StateAuthFail = "auth_failed" // disabled; re-probed after a growing backoff or on a settings change
	StateClosed   = "closed"
)

// breaker is a per-provider circuit breaker. Authentication failures open it
// until a single background probe (see pool.authProbe) logs in again; probes
// run after authMin, doubling per failed probe up to authMax, so a provider
// whose auth service has a transient outage heals by itself while bad
// credentials cost one login attempt per backoff. A new pool (created when
// settings change) starts closed.
// Connection failures open it for an exponentially growing backoff once
// failThreshold consecutive connections or commands have failed; a successful
// article/STAT response resets it. A successful login alone does not.
type breaker struct {
	mu            sync.Mutex
	now           func() time.Time
	minBackoff    time.Duration
	maxBackoff    time.Duration
	failThreshold int

	authMin, authMax time.Duration

	changed      chan struct{}
	networkProbe bool // at most one command tests an expired network backoff
	failures     int
	until        time.Time
	authErr      error
	authFails    int       // consecutive failed logins (initial failure + probes)
	authUntil    time.Time // next auth probe is due
	probing      bool      // an auth probe is in flight
	lastErr      error
	lastAt       time.Time
}

func newBreaker(minB, maxB time.Duration, threshold int, now func() time.Time) *breaker {
	if threshold < 1 {
		threshold = 1
	}
	return &breaker{now: now, minBackoff: minB, maxBackoff: maxB, failThreshold: threshold,
		authMin: DefaultAuthRetryMin, authMax: DefaultAuthRetryMax, changed: make(chan struct{})}
}

// allow returns nil if the provider may be used now.
func (b *breaker) allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.authErr != nil {
		return fmt.Errorf("%w: %v", ErrProviderDisabled, b.authErr)
	}
	if now := b.now(); now.Before(b.until) {
		return fmt.Errorf("%w for %s: %v", ErrProviderBackoff, b.until.Sub(now).Round(time.Millisecond), b.lastErr)
	}
	return nil
}

// admit reserves a single half-open recovery attempt after a network backoff.
// allow remains a read-only check for health/status/warming paths.
func (b *breaker) admit() (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.authErr != nil {
		return false, fmt.Errorf("%w: %v", ErrProviderDisabled, b.authErr)
	}
	if b.now().Before(b.until) || b.networkProbe {
		return false, ErrProviderBackoff
	}
	if b.failures >= b.failThreshold {
		b.networkProbe = true
		return true, nil
	}
	return false, nil
}

func (b *breaker) releaseProbe() {
	b.mu.Lock()
	b.networkProbe = false
	b.signalLocked()
	b.mu.Unlock()
}

func (b *breaker) success() {
	b.mu.Lock()
	b.failures = 0
	b.until = time.Time{}
	b.signalLocked()
	b.mu.Unlock()
}

// cappedDouble doubles d, clamped to [lo, hi].
func cappedDouble(d, lo, hi time.Duration) time.Duration {
	return min(max(d*2, lo), hi)
}

// connFailure records a failed dial and returns the backoff now in force.
func (b *breaker) connFailure(err error) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	b.lastErr, b.lastAt = err, b.now()
	if b.failures < b.failThreshold {
		return 0
	}
	d := b.minBackoff
	for i := b.failThreshold; i < b.failures && d < b.maxBackoff; i++ {
		d = cappedDouble(d, b.minBackoff, b.maxBackoff)
	}
	if d > b.maxBackoff {
		d = b.maxBackoff
	}
	b.until = b.now().Add(d)
	b.signalLocked()
	return d
}

// authFailure disables the provider after a rejected login and returns the
// delay until the next probe. Failures reported while already disabled and
// not probing (concurrent dials racing the first one) do not grow the
// backoff.
func (b *breaker) authFailure(err error) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.lastErr, b.lastAt = err, now
	if b.authErr != nil && !b.probing {
		return max(b.authUntil.Sub(now), 0)
	}
	b.probing = false
	b.authErr = err
	b.authFails++
	d := b.authMin
	for i := 1; i < b.authFails && d < b.authMax; i++ {
		d = cappedDouble(d, b.authMin, b.authMax)
	}
	d = min(d, b.authMax)
	b.authUntil = now.Add(d)
	return d
}

// authProbeDue reports whether an auth-disabled provider is due a login
// probe, and if so claims it so only one runs at a time.
func (b *breaker) authProbeDue() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.authErr == nil || b.probing || b.now().Before(b.authUntil) {
		return false
	}
	b.probing = true
	return true
}

// authRecovered re-enables the provider after a successful login probe.
func (b *breaker) authRecovered() {
	b.mu.Lock()
	b.authErr, b.authFails, b.authUntil, b.probing = nil, 0, time.Time{}, false
	b.mu.Unlock()
}

// authProbeInconclusive ends a probe that failed for a reason other than
// the credentials (e.g. the network is down); it is retried after authMin
// without growing the backoff.
func (b *breaker) authProbeInconclusive(err error) {
	b.mu.Lock()
	b.probing = false
	b.authUntil = b.now().Add(b.authMin)
	b.lastErr, b.lastAt = err, b.now()
	b.mu.Unlock()
}

// noteError records a non-tripping error for statistics.
func (b *breaker) noteError(err error) {
	b.mu.Lock()
	b.lastErr, b.lastAt = err, b.now()
	b.mu.Unlock()
}

func (b *breaker) state() (state string, until time.Time, lastErr error, lastAt time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.authErr != nil:
		return StateAuthFail, b.authUntil, b.lastErr, b.lastAt
	case b.now().Before(b.until):
		state = StateBackoff
	default:
		state = StateOK
	}
	return state, b.until, b.lastErr, b.lastAt
}

// signalLocked wakes requests whose last usable provider is recovering.
func (b *breaker) signalLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *breaker) recoveryDelay() (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.authErr != nil || (b.failures >= b.failThreshold && isTimeoutError(b.lastErr)) {
		return 0, false
	}
	return max(0, b.until.Sub(b.now())), true
}

// waitReady does not claim a probe. The connection admission path still grants
// exactly one half-open probe; peers sleep again until it completes.
func (b *breaker) waitReady(ctx context.Context) error {
	for {
		b.mu.Lock()
		if b.authErr != nil {
			b.mu.Unlock()
			return ErrProviderDisabled
		}
		delay := b.until.Sub(b.now())
		if delay <= 0 && !b.networkProbe {
			b.mu.Unlock()
			return nil
		}
		changed := b.changed
		if delay <= 0 {
			delay = time.Minute
		} // wake on probe completion, bounded by ctx
		b.mu.Unlock()
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}
