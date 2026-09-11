package scenarios

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/temporalio/omes/loadgen"
	"github.com/temporalio/omes/loadgen/kitchensink"
	"go.temporal.io/api/workflowservice/v1"
)

func init() {
	loadgen.MustRegisterScenario(loadgen.Scenario{
		Description: "Run a certain number of state transitions per second. Requires --duration; any iteration " +
			"configuration is ignored. For example: run-scenario-with-worker --scenario state_transitions_steady " +
			"--language go --embedded-server --duration 5m --option state-transitions-per-second=3",
		Options: func(o *loadgen.OptionSet) {
			o.Int("state-transitions-per-second", 0, "State transitions to sustain per second.")
			o.MarkRequired("state-transitions-per-second")
		},
		ExecutorFn: func() loadgen.Executor {
			return loadgen.ExecutorFunc(func(ctx context.Context, runOptions loadgen.ScenarioInfo) error {
				return (&stateTransitionsSteady{runOptions}).run(ctx)
			})
		},
	})
}

type stateTransitionsSteady struct{ loadgen.ScenarioInfo }

func (s *stateTransitionsSteady) run(ctx context.Context) error {
	// The goal here is to meet a certain number of state transitions per second.
	// For us this means a certain number of workflows per second. So we must
	// first execute a basic workflow (i.e. with a simple activity) and get the
	// number of state transitions it took

	// TODO(cretz): Note, this is a known naive approach because if workflows are
	// backed up and/or slow down this won't match.

	if s.Configuration.Duration == 0 {
		return fmt.Errorf("duration required for this scenario")
	}
	stateTransitionsPerSecond := s.OptionInt("state-transitions-per-second")
	if stateTransitionsPerSecond == 0 {
		return fmt.Errorf("state-transitions-per-second scenario option required")
	}
	durationPerStateTransition := time.Second / time.Duration(stateTransitionsPerSecond)
	s.Logger.Infof(
		"State transitions per second is %v, which means %v between every state transition",
		stateTransitionsPerSecond,
		durationPerStateTransition,
	)

	// Execute initial workflow and get the transition count
	workflowParams := &kitchensink.WorkflowInput{
		InitialActions: []*kitchensink.ActionSet{
			kitchensink.NoOpSingleActivityActionSet(),
		},
	}
	workflowRun, err := s.Client.ExecuteWorkflow(
		ctx,
		s.NewRun(0).DefaultStartWorkflowOptions(),
		"kitchenSink",
		workflowParams,
	)
	if err != nil {
		return fmt.Errorf("failed starting initial workflow: %w", err)
	} else if err := workflowRun.Get(ctx, nil); err != nil {
		return fmt.Errorf("failed executing initial workflow: %w", err)
	}
	resp, err := s.Client.DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	if err != nil {
		return fmt.Errorf("failed describing workflow: %w", err)
	}
	stateTransitionsPerWorkflow := resp.WorkflowExecutionInfo.StateTransitionCount
	workflowStartInterval := durationPerStateTransition * time.Duration(stateTransitionsPerWorkflow)
	s.Logger.Infof(
		"Simple workflow takes %v state transitions, which means we need to start a workflow every %v. "+
			"Running for %v now...",
		stateTransitionsPerWorkflow,
		workflowStartInterval,
		s.Configuration.Duration,
	)

	// Start a workflow every X interval until duration reached or there are N
	// start failures in a row

	const maxConsecutiveErrors = 5
	errCh := make(chan error, 10000)
	ticker := time.NewTicker(workflowStartInterval)
	defer ticker.Stop()
	iter := 1
	var startWG sync.WaitGroup

	// Completions are consumed on their own goroutine, so that the pacing loop below waits on
	// the ticker and nothing else. With both channels in one select, a completion that won the
	// race left the tick sitting in the ticker's one-slot channel, and the next tick to arrive
	// while the loop was still on that branch was dropped -- time.Ticker drops ticks for slow
	// receivers by design. At 600 starts a second the loop had slack and reached 96 % of the
	// ordered rate; at 1200 it reached 72 %, with the cluster under test idle on every axis
	// (measured 2026-09-11 on the log-first stand: 862 starts/s against 1200 ordered). The
	// consecutive-error stop is unchanged: it is still the scenario's one way to fail, and a
	// generator that dies rather than degrades is the behaviour the campaign relies on.
	fatalCh := make(chan error, 1)
	go func() {
		var consecutiveErrCount int
		for {
			select {
			case <-ctx.Done():
				return
			case err := <-errCh:
				if err == nil {
					consecutiveErrCount = 0
					continue
				}
				consecutiveErrCount++
				if consecutiveErrCount >= maxConsecutiveErrors {
					select {
					case fatalCh <- fmt.Errorf("got %v consecutive errors, most recent: %w", maxConsecutiveErrors, err):
					default:
					}
					return
				}
			}
		}
	}()

	// The rate is the contract, not the tick count. A time.Ticker holds at most one pending
	// tick, so any delay in this goroutine longer than the interval -- a collection, the
	// scheduler on a shared node -- loses a tick at the timer, before the select ever sees it.
	// At an 833 us interval on a four-core node shared with three workers that was 15 % of
	// the starts, after the select above was already fixed. So each tick starts as many
	// workflows as the clock says are owed by now, not one: a lost tick is caught up on the
	// next, and the ordered rate holds on average whatever the jitter. If the cluster cannot
	// take it, start latency rises and the consecutive-error stop still ends the run.
	begin := time.Now()
	started := 0
	for time.Since(begin) < s.Configuration.Duration {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-fatalCh:
			return err
		case <-ticker.C:
			owed := int(time.Since(begin) / workflowStartInterval)
			for ; started < owed; started++ {
				iter = started + 1
				s.Logger.Debugf("Running iteration %v", iter)
				startWG.Add(1)
				go func(iter int) {
					defer startWG.Done()
					_, err := s.Client.ExecuteWorkflow(
						ctx,
						s.NewRun(iter).DefaultStartWorkflowOptions(),
						"kitchenSink",
						workflowParams,
					)
					// Only send to err ch if there is room just in case
					select {
					case errCh <- err:
					default:
					}
				}(iter)
			}
		}
	}

	// We are waiting to let workflows complete here, knowing that this part of
	// the scenario is not steady state transitions. We will wait a minute max to
	// confirm all workflows on the task queue are no longer running.
	s.Logger.Infof("Run complete, ran %v iterations, waiting on all workflows to complete", iter)
	// First, wait for all starts to have started (they are done in goroutine)
	startWG.Wait()
	return loadgen.MinVisibilityCountEventually(
		ctx,
		s.ScenarioInfo,
		&workflowservice.CountWorkflowExecutionsRequest{
			Namespace: s.Namespace,
			Query: fmt.Sprintf("TaskQueue = %q and ExecutionStatus = 'Running'",
				loadgen.TaskQueueForRun(s.RunID)),
		},
		0,
		time.Minute,
	)
}
