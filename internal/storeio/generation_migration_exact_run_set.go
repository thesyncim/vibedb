package storeio

import "fmt"

// GenerationMigrationExactRunSet is a fixed 64-level binary merge ladder.
// Discontinuous staging extents never require a resident span vector: each
// completed contiguous run is merged at its size level immediately, bounding
// descriptors and rewrite work to O(log runs).
type GenerationMigrationExactRunSet struct {
	sink      PrimaryGraphBuildSink
	read      GenerationMigrationExactRunReader
	pageBytes uint32
	levels    [64]GenerationMigrationExactRunRegion
}

func NewGenerationMigrationExactRunSet(
	sink PrimaryGraphBuildSink,
	read GenerationMigrationExactRunReader,
	pageBytes uint32,
) (*GenerationMigrationExactRunSet, error) {
	if sink == nil || read == nil || !validPhysicalPageSize(pageBytes) ||
		pageBytes > uint32(sink.MaxBuildPageBytes()) {
		return nil, fmt.Errorf("%w: exact run set", ErrInvalidWrite)
	}
	return &GenerationMigrationExactRunSet{sink: sink, read: read, pageBytes: pageBytes}, nil
}

func (s *GenerationMigrationExactRunSet) Add(run GenerationMigrationExactRunRegion) error {
	if s == nil || run.Runs != 1 || run.Pages == 0 ||
		run.First.Kind != PageMigrationExactRun || run.First.Length != s.pageBytes {
		return fmt.Errorf("%w: exact run set add", ErrInvalidWrite)
	}
	for level := range s.levels {
		if s.levels[level].Pages == 0 {
			s.levels[level] = run
			return nil
		}
		merged, err := MergeGenerationMigrationExactRunPair(
			s.sink, s.read, s.levels[level], run, s.pageBytes,
		)
		if err != nil {
			return err
		}
		s.levels[level] = GenerationMigrationExactRunRegion{}
		run = merged
	}
	return fmt.Errorf("%w: exact run set levels exhausted", ErrInvalidWrite)
}

func (s *GenerationMigrationExactRunSet) Finish() (GenerationMigrationExactRunRegion, error) {
	if s == nil {
		return GenerationMigrationExactRunRegion{}, ErrInvalidWrite
	}
	var result GenerationMigrationExactRunRegion
	for level := range s.levels {
		if s.levels[level].Pages == 0 {
			continue
		}
		if result.Pages == 0 {
			result = s.levels[level]
			continue
		}
		merged, err := MergeGenerationMigrationExactRunPair(
			s.sink, s.read, s.levels[level], result, s.pageBytes,
		)
		if err != nil {
			return GenerationMigrationExactRunRegion{}, err
		}
		result = merged
	}
	if result.Pages == 0 {
		return GenerationMigrationExactRunRegion{}, nil
	}
	return result, nil
}

func MergeGenerationMigrationExactRunPair(
	sink PrimaryGraphBuildSink,
	read GenerationMigrationExactRunReader,
	left, right GenerationMigrationExactRunRegion,
	pageBytes uint32,
) (GenerationMigrationExactRunRegion, error) {
	if sink == nil || read == nil || left.Runs != 1 || right.Runs != 1 ||
		left.Pages == 0 || right.Pages == 0 || left.First.Length != pageBytes ||
		right.First.Length != pageBytes {
		return GenerationMigrationExactRunRegion{}, fmt.Errorf("%w: exact run pair", ErrInvalidWrite)
	}
	if contiguous, ok := sink.(interface{ EnsureContiguousBuildBytes(uint64) error }); ok {
		if left.Pages > (^uint64(0)-right.Pages) ||
			left.Pages+right.Pages > ^uint64(0)/uint64(pageBytes) {
			return GenerationMigrationExactRunRegion{}, ErrInvalidWrite
		}
		if err := contiguous.EnsureContiguousBuildBytes((left.Pages + right.Pages) * uint64(pageBytes)); err != nil {
			return GenerationMigrationExactRunRegion{}, err
		}
	}
	scratch := make([]byte, int(pageBytes)*3)
	leftSpan, next, err := scanGenerationMigrationExactRunSpan(
		read, left, 0, scratch[:pageBytes], sink.StoreIdentity(), sink.BuildGeneration(),
	)
	if err != nil || next != left.Pages {
		if err != nil {
			return GenerationMigrationExactRunRegion{}, err
		}
		return GenerationMigrationExactRunRegion{}, ErrGenerationMigrationManifestCorrupt
	}
	rightSpan, next, err := scanGenerationMigrationExactRunSpan(
		read, right, 0, scratch[:pageBytes], sink.StoreIdentity(), sink.BuildGeneration(),
	)
	if err != nil || next != right.Pages {
		if err != nil {
			return GenerationMigrationExactRunRegion{}, err
		}
		return GenerationMigrationExactRunRegion{}, ErrGenerationMigrationManifestCorrupt
	}
	cursors := [2]generationMigrationExactRunCursor{
		{read: read, span: leftSpan, page: scratch[pageBytes : 2*pageBytes]},
		{read: read, span: rightSpan, page: scratch[2*pageBytes : 3*pageBytes]},
	}
	for index := range cursors {
		if err := cursors[index].start(sink.StoreIdentity(), sink.BuildGeneration()); err != nil {
			return GenerationMigrationExactRunRegion{}, err
		}
	}
	var output GenerationMigrationExactRunRegion
	writer := generationMigrationExactOrderedRunWriter{
		sink: sink, pageBytes: pageBytes, runID: 1, region: &output,
		records:  make([]GenerationMigrationExactRunRecord, 0, int(pageBytes)/generationMigrationExactRunRecordBytes),
		keyArena: make([]byte, 0, int(pageBytes)),
	}
	for cursors[0].valid || cursors[1].valid {
		selected := 0
		if !cursors[0].valid || cursors[1].valid && compareGenerationMigrationExactRunRecord(cursors[1].current, cursors[0].current) < 0 {
			selected = 1
		}
		if err := writer.add(cursors[selected].current); err != nil {
			return GenerationMigrationExactRunRegion{}, err
		}
		if err := cursors[selected].advance(sink.StoreIdentity(), sink.BuildGeneration()); err != nil {
			return GenerationMigrationExactRunRegion{}, err
		}
	}
	if err := writer.finish(); err != nil {
		return GenerationMigrationExactRunRegion{}, err
	}
	return output, nil
}
