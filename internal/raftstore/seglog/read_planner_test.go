package seglog

import (
	"errors"
	"math"
	"testing"
)

func TestReadPlannerBoundedAmplificationAndScatter(t *testing.T) {
	extents := []PhysicalExtent{
		{SegmentID: 1, Offset: 100, Bytes: 20, First: 1, Last: 4},
		{SegmentID: 1, Offset: 124, Bytes: 20, First: 5, Last: 8},
		{SegmentID: 1, Offset: 200, Bytes: 10, First: 9, Last: 9},
		{SegmentID: 2, Offset: 128, Bytes: 30, First: 10, Last: 12},
	}
	var workspace ReadPlanWorkspace
	spans, extra, err := workspace.Plan(extents, 8)
	if err != nil || len(spans) != 3 || extra != 4 || spans[0].Offset != 100 || spans[0].Bytes != 44 || spans[0].Extents != 2 {
		t.Fatalf("plan = %#v extra=%d err=%v", spans, extra, err)
	}
	if got := testing.AllocsPerRun(1000, func() {
		if _, _, planErr := workspace.Plan(extents, 8); planErr != nil {
			panic(planErr)
		}
	}); got != 0 {
		t.Fatalf("plan allocs/run = %v", got)
	}
}

func TestReadPlannerRejectsOverlapReverseAndOverflow(t *testing.T) {
	for name, extents := range map[string][]PhysicalExtent{
		"overlap":           {{SegmentID: 1, Offset: 10, Bytes: 10, First: 1, Last: 1}, {SegmentID: 1, Offset: 19, Bytes: 1, First: 2, Last: 2}},
		"reverse":           {{SegmentID: 1, Offset: 20, Bytes: 1, First: 1, Last: 1}, {SegmentID: 1, Offset: 10, Bytes: 1, First: 2, Last: 2}},
		"geometry-overflow": {{SegmentID: 1, Offset: math.MaxUint64, Bytes: 1, First: 1, Last: 1}},
		"index-overflow":    {{SegmentID: 1, Offset: 1, Bytes: 1, First: math.MaxUint64, Last: math.MaxUint64}, {SegmentID: 2, Offset: 1, Bytes: 1, First: 1, Last: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			var workspace ReadPlanWorkspace
			if _, _, err := workspace.Plan(extents, math.MaxUint64); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestReadPlannerPaginationAndGlobalBudget(t *testing.T) {
	extents := make([]PhysicalExtent, MaxPlannedExtents+10)
	for i := range extents {
		extents[i] = PhysicalExtent{SegmentID: uint64(i + 1), Offset: 1, Bytes: 1, First: uint64(i + 1), Last: uint64(i + 1)}
	}
	var workspace ReadPlanWorkspace
	spans, extra, next, err := workspace.PlanPage(extents, 7, 0)
	if err != nil || len(spans) != MaxPlannedExtents || extra != 0 || next != MaxPlannedExtents {
		t.Fatalf("first page spans=%d extra=%d next=%d err=%v", len(spans), extra, next, err)
	}
	spans, _, next, err = workspace.PlanPage(extents, 7, next)
	if err != nil || len(spans) != 10 || next != len(extents) {
		t.Fatalf("second page spans=%d next=%d err=%v", len(spans), next, err)
	}
	budgeted := []PhysicalExtent{{SegmentID: 1, Offset: 0, Bytes: 4, First: 1, Last: 1}, {SegmentID: 1, Offset: 8, Bytes: 4, First: 2, Last: 2}, {SegmentID: 1, Offset: 16, Bytes: 4, First: 3, Last: 3}}
	spans, extra, err = workspace.Plan(budgeted, 4)
	if err != nil || len(spans) != 2 || extra != 4 {
		t.Fatalf("budget spans=%#v extra=%d err=%v", spans, extra, err)
	}
}
