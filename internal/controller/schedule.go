/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Scheduling policy. These are pure functions over (schedule, times) with no
// client and no globals, which is what makes the whole time-based matrix
// testable as a table instead of against a cluster.

const (
	// MinScheduleInterval is the shortest permitted gap between runs.
	//
	// This is load-bearing for Job naming, not arbitrary strictness. JobName
	// uses minute granularity, so two slots inside the same minute would
	// produce the same Job name, the second Create would return AlreadyExists,
	// and the run would be silently swallowed instead of executed. Rejecting
	// sub-minute schedules keeps the deterministic-name guarantee honest.
	// Changing this requires changing JobName in job.go, and vice versa.
	MinScheduleInterval = time.Minute

	// maxMissedSlots bounds the walk over missed slots, so a pathological
	// anchor cannot pin the reconcile loop.
	maxMissedSlots = 10000
)

// ScheduleErrorReason classifies why a schedule was rejected. It is surfaced
// verbatim as the reason on the ScheduleValid condition.
type ScheduleErrorReason string

const (
	// ReasonParseError means the expression is not valid cron.
	ReasonParseError ScheduleErrorReason = "ParseError"
	// ReasonUnsatisfiable means the expression parses but can never fire, such
	// as "0 0 30 2 *" (February 30th).
	ReasonUnsatisfiable ScheduleErrorReason = "Unsatisfiable"
	// ReasonIntervalTooShort means the expression fires more often than
	// MinScheduleInterval.
	ReasonIntervalTooShort ScheduleErrorReason = "IntervalTooShort"
)

// ScheduleError is a rejection from ValidateSchedule.
type ScheduleError struct {
	Reason  ScheduleErrorReason
	Message string
}

func (e *ScheduleError) Error() string { return e.Message }

// AsScheduleError extracts a *ScheduleError from err, if it is one.
func AsScheduleError(err error) (*ScheduleError, bool) {
	var se *ScheduleError
	ok := errors.As(err, &se)
	return se, ok
}

// ErrTooManyMissed is returned by DueSlot when the walk from the anchor exceeds
// maxMissedSlots. The caller resets the anchor to now and warns.
var ErrTooManyMissed = errors.New("too many missed schedule slots")

// ValidateSchedule parses spec.schedule and rejects schedules that parse but
// cannot work.
//
// "Parses successfully" is not the same as "can ever run": conflating the two
// produces a Task that sits silently idle forever, which is the worst possible
// failure mode because nothing reports it. There are three distinct rejections,
// each surfaced with its own reason on the ScheduleValid condition.
func ValidateSchedule(spec string, now time.Time) (cron.Schedule, error) {
	sched, err := cron.ParseStandard(spec)
	if err != nil {
		return nil, &ScheduleError{
			Reason:  ReasonParseError,
			Message: fmt.Sprintf("schedule %q is not a valid cron expression: %v", spec, err),
		}
	}

	// robfig/cron searches five years ahead and returns the zero time if it
	// finds nothing. That is how an impossible date such as February 30th is
	// detected: it parses cleanly but can never be satisfied.
	first := sched.Next(now)
	if first.IsZero() {
		return nil, &ScheduleError{
			Reason: ReasonUnsatisfiable,
			Message: fmt.Sprintf(
				"schedule %q parses but can never run; check for an impossible date such as February 30th", spec),
		}
	}

	second := sched.Next(first)
	if !second.IsZero() && second.Sub(first) < MinScheduleInterval {
		return nil, &ScheduleError{
			Reason: ReasonIntervalTooShort,
			Message: fmt.Sprintf(
				"schedule %q fires every %s, which is faster than the %s minimum",
				spec, second.Sub(first), MinScheduleInterval),
		}
	}

	return sched, nil
}

// NextSlot returns the first slot strictly after the given time.
func NextSlot(sched cron.Schedule, after time.Time) time.Time {
	return sched.Next(after)
}

// SlotDecision is the outcome of asking which slot is due.
type SlotDecision struct {
	// Slot is the slot to act on, valid only when Due is true.
	Slot time.Time
	Due  bool
	// SkippedByDeadline reports that a slot was genuinely due but was dropped
	// because startingDeadlineSeconds had elapsed. Distinguishing this from
	// "nothing was due" is what makes a skipped run observable instead of
	// silent.
	SkippedByDeadline bool
}

// DueSlot returns the slot that should run now, if any.
//
// The anchor is status.lastScheduleTime, or the Task's creationTimestamp for a
// Task that has never run, so a Task created at 10:02 with */5 first runs at
// 10:05 rather than immediately. startingDeadlineSeconds moves the anchor
// forward to now-deadline, bounding how late a missed slot may still run.
//
// The due slot is the newest slot in (anchor, now]: older missed slots collapse
// into it, so a three-hour outage produces one run rather than a burst of 36.
func DueSlot(
	sched cron.Schedule,
	anchor, now time.Time,
	startingDeadlineSeconds *int64,
) (SlotDecision, error) {
	effectiveAnchor := anchor
	deadlineApplied := false
	if startingDeadlineSeconds != nil {
		earliest := now.Add(-time.Duration(*startingDeadlineSeconds) * time.Second)
		if earliest.After(anchor) {
			effectiveAnchor = earliest
			deadlineApplied = true
		}
	}

	newest := func(from time.Time) (time.Time, error) {
		var last time.Time
		count := 0
		for t := sched.Next(from); !t.IsZero() && !t.After(now); t = sched.Next(t) {
			last = t
			count++
			if count > maxMissedSlots {
				return time.Time{}, ErrTooManyMissed
			}
		}
		return last, nil
	}

	last, err := newest(effectiveAnchor)
	if err != nil {
		return SlotDecision{}, err
	}
	if !last.IsZero() {
		return SlotDecision{Slot: last, Due: true}, nil
	}

	// Nothing is due. If the deadline moved the anchor forward, find out whether
	// a slot would have been due without it, so the drop can be reported rather
	// than passing silently.
	if deadlineApplied {
		missed, missedErr := newest(anchor)
		if missedErr == nil && !missed.IsZero() {
			return SlotDecision{SkippedByDeadline: true}, nil
		}
	}
	return SlotDecision{}, nil
}
