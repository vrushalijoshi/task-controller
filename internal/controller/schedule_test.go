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
	"testing"
	"time"
)

// base is a fixed instant so every time-based assertion is deterministic.
var base = time.Date(2026, 10, 7, 17, 2, 0, 0, time.UTC)

func TestValidateSchedule(t *testing.T) {
	tests := []struct {
		name       string
		schedule   string
		wantReason ScheduleErrorReason // empty means the schedule is accepted
	}{
		{name: "every five minutes", schedule: everyFiveMinutes},
		{name: "hourly descriptor", schedule: "@hourly"},
		{name: "daily descriptor", schedule: "@daily"},
		{name: "every ten minutes", schedule: "@every 10m"},
		{name: "exactly the minimum interval", schedule: "@every 1m"},
		{name: "leap day is satisfiable", schedule: "0 0 29 2 *"},

		{name: "too few fields", schedule: tooFewCronFields, wantReason: ReasonParseError},
		{name: "minute out of range", schedule: "99 * * * *", wantReason: ReasonParseError},
		{name: "gibberish", schedule: "not-a-cron", wantReason: ReasonParseError},

		// Parses cleanly, but no such date exists. Without this check the Task
		// would sit idle forever with nothing reporting why.
		{name: "February 30th", schedule: "0 0 30 2 *", wantReason: ReasonUnsatisfiable},
		{name: "April 31st", schedule: "0 0 31 4 *", wantReason: ReasonUnsatisfiable},

		// Sub-minute schedules would collide on the minute-granularity Job name.
		{name: "every thirty seconds", schedule: "@every 30s", wantReason: ReasonIntervalTooShort},
		{name: "every second", schedule: "@every 1s", wantReason: ReasonIntervalTooShort},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sched, err := ValidateSchedule(tc.schedule, base)

			if tc.wantReason == "" {
				if err != nil {
					t.Fatalf("ValidateSchedule(%q) = error %v, want accepted", tc.schedule, err)
				}
				if sched == nil {
					t.Fatalf("ValidateSchedule(%q) returned a nil schedule", tc.schedule)
				}
				return
			}

			if err == nil {
				t.Fatalf("ValidateSchedule(%q) = accepted, want reason %s", tc.schedule, tc.wantReason)
			}
			se, ok := AsScheduleError(err)
			if !ok {
				t.Fatalf("ValidateSchedule(%q) returned %T, want *ScheduleError", tc.schedule, err)
			}
			if se.Reason != tc.wantReason {
				t.Errorf("reason = %s, want %s (message: %s)", se.Reason, tc.wantReason, se.Message)
			}
		})
	}
}

func TestDueSlot(t *testing.T) {
	everyFive := mustSchedule(t, everyFiveMinutes)

	tests := []struct {
		name        string
		anchor      time.Time
		now         time.Time
		deadline    *int64
		wantSlot    time.Time
		wantDue     bool
		wantSkipped bool
	}{
		{
			// A Task created at 10:02 with */5 first runs at 10:05, not
			// immediately: the anchor is the creation time.
			name:    "before the first slot",
			anchor:  time.Date(2026, 10, 7, 10, 2, 0, 0, time.UTC),
			now:     time.Date(2026, 10, 7, 10, 4, 0, 0, time.UTC),
			wantDue: false,
		},
		{
			name:     "slot reached",
			anchor:   time.Date(2026, 10, 7, 10, 2, 0, 0, time.UTC),
			now:      time.Date(2026, 10, 7, 10, 5, 0, 0, time.UTC),
			wantSlot: time.Date(2026, 10, 7, 10, 5, 0, 0, time.UTC),
			wantDue:  true,
		},
		{
			// Three hours of downtime must produce one run, not 36.
			name:     "missed slots collapse into the newest",
			anchor:   time.Date(2026, 10, 7, 7, 0, 0, 0, time.UTC),
			now:      time.Date(2026, 10, 7, 10, 2, 0, 0, time.UTC),
			wantSlot: time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC),
			wantDue:  true,
		},
		{
			// The deadline moves the anchor forward to now-deadline, so a slot
			// missed by more than that is skipped entirely.
			name:     "starting deadline skips a stale slot",
			anchor:   time.Date(2026, 10, 7, 7, 0, 0, 0, time.UTC),
			now:      time.Date(2026, 10, 7, 10, 2, 0, 0, time.UTC),
			deadline: ptrInt64(60),
			wantDue:  false,
			// Reported rather than silent: a slot was genuinely due and dropped.
			wantSkipped: true,
		},
		{
			name:     "starting deadline still allows a recent slot",
			anchor:   time.Date(2026, 10, 7, 7, 0, 0, 0, time.UTC),
			now:      time.Date(2026, 10, 7, 10, 2, 0, 0, time.UTC),
			deadline: ptrInt64(300),
			wantSlot: time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC),
			wantDue:  true,
		},
		{
			name:    "anchor equal to now",
			anchor:  time.Date(2026, 10, 7, 10, 5, 0, 0, time.UTC),
			now:     time.Date(2026, 10, 7, 10, 5, 0, 0, time.UTC),
			wantDue: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DueSlot(everyFive, tc.anchor, tc.now, tc.deadline)
			if err != nil {
				t.Fatalf("DueSlot returned an unexpected error: %v", err)
			}
			if got.Due != tc.wantDue {
				t.Fatalf("due = %v, want %v (slot %v)", got.Due, tc.wantDue, got.Slot)
			}
			if got.Due && !got.Slot.Equal(tc.wantSlot) {
				t.Errorf("slot = %v, want %v", got.Slot, tc.wantSlot)
			}
			if got.SkippedByDeadline != tc.wantSkipped {
				t.Errorf("skippedByDeadline = %v, want %v", got.SkippedByDeadline, tc.wantSkipped)
			}
		})
	}
}

// A pathological anchor must not pin the reconcile loop walking slots forever.
func TestDueSlotTooManyMissed(t *testing.T) {
	everyMinute := mustSchedule(t, "* * * * *")
	anchor := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	_, err := DueSlot(everyMinute, anchor, base, nil)
	if !errors.Is(err, ErrTooManyMissed) {
		t.Fatalf("DueSlot error = %v, want ErrTooManyMissed", err)
	}
}

func TestNextSlot(t *testing.T) {
	everyFive := mustSchedule(t, everyFiveMinutes)
	got := NextSlot(everyFive, time.Date(2026, 10, 7, 10, 2, 0, 0, time.UTC))
	want := time.Date(2026, 10, 7, 10, 5, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("NextSlot = %v, want %v", got, want)
	}
}

func mustSchedule(t *testing.T, spec string) interface {
	Next(time.Time) time.Time
} {
	t.Helper()
	s, err := ValidateSchedule(spec, base)
	if err != nil {
		t.Fatalf("ValidateSchedule(%q): %v", spec, err)
	}
	return s
}

//nolint:modernize // &v on a parameter is the clearest spelling here.
func ptrInt64(v int64) *int64 { return &v }
