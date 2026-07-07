package scheduler

import (
	"sync"
	"testing"
	"time"
)

// TestHotKnobsRaceSafe drives the settings hot-apply surface under the race
// detector: a running scheduler sweeps (reading budgetCap/maxParallelN/
// backoffDelay on its loop goroutine) while a writer goroutine hammers the
// Set* methods from another goroutine. Any unguarded access trips `-race`.
func TestHotKnobsRaceSafe(t *testing.T) {
	f := newFixture(t)
	d := newFakeDriver()
	// A live job with a DAG so the sweep actually reads the hot knobs each tick.
	f.triagedJob(t, fanoutPlan())
	s := startScheduler(t, f.cli, d, func(o *Options) {
		o.Interval = time.Millisecond
		o.BudgetUSD = 100
		o.MaxParallel = 2
		o.Backoff = 10 * time.Millisecond
	})

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer: raise/lower the hot knobs continuously.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			i++
			s.SetBudgetUSD(float64(i%1000) + 1)
			s.SetMaxParallel(i%4 + 1)
			s.SetBackoff(time.Duration(i%50+1) * time.Millisecond)
		}
	}()

	// Reader: also read the accessors directly (belt-and-braces alongside the
	// loop's own reads).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.budgetCap()
			_ = s.maxParallelN()
			_ = s.backoffDelay()
		}
	}()

	time.Sleep(60 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Sanity: the last write is observable and never touched spent.
	s.SetBudgetUSD(42)
	if got := s.budgetCap(); got != 42 {
		t.Fatalf("budgetCap = %v, want 42", got)
	}
}

// TestSetBudgetDoesNotTouchSpent pins that a live budget raise preserves
// spend-to-date (the restart-resets-spend footgun must not fire on a hot raise).
func TestSetBudgetDoesNotTouchSpent(t *testing.T) {
	f := newFixture(t)
	s, err := New(f.cli, Options{Driver: newFakeDriver(), BudgetUSD: 5})
	if err != nil {
		t.Fatal(err)
	}
	s.spent = 3.5 // as if a run already accrued
	s.SetBudgetUSD(50)
	if s.spent != 3.5 {
		t.Fatalf("spent changed on budget raise: %v, want 3.5", s.spent)
	}
	if s.budgetCap() != 50 {
		t.Fatalf("budgetCap = %v, want 50", s.budgetCap())
	}
}
