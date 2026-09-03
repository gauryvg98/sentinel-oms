package main

import "testing"

func TestReadStanceSpotsExposureWithNothingUnderIt(t *testing.T) {
	cases := []struct {
		note          string
		held, stopped bool
		why           string
	}{
		{"FLAT bar 81661 fast 81181 slow 78699 holding 0", false, false,
			"flat is not exposure"},
		{"LONG bar 81661 fast 81181 slow 78699 holding 0.002", true, false,
			"this is the condition that ran for an hour on 2026-09-03"},
		{"LONG bar 81661 fast 81181 slow 78699 holding 0.002 stop 81252.6", true, true,
			"held and protected"},
		{"SHORT bar 81661 fast 81181 slow 78699 holding -0.002 stop 81900", true, true,
			"a short is exposure too"},
		{"SHORT bar 81661 fast 81181 slow 78699 holding -0.002", true, false,
			"an unprotected short"},
		{"warming up", false, false, "no opinion yet"},
	}
	for _, c := range cases {
		held, stopped := readStance(c.note)
		if held != c.held || stopped != c.stopped {
			t.Errorf("%q -> held=%v stopped=%v, want %v/%v (%s)",
				c.note, held, stopped, c.held, c.stopped, c.why)
		}
	}
}

func TestAnUnreadableNoteFailsTowardsPaging(t *testing.T) {
	// If the stance format ever changes, reporting "no stop" wakes someone.
	// Reporting "stop" would let a real naked position pass unnoticed, which
	// is the failure this alert exists to prevent.
	held, stopped := readStance("something entirely different holding 0.002")
	if !held || stopped {
		t.Errorf("held=%v stopped=%v, want held with no stop", held, stopped)
	}
}
