package sql

// SequenceOptions holds parsed sequence options for CREATE SEQUENCE,
// ALTER SEQUENCE, and GENERATED ... AS IDENTITY ( ... ).
type SequenceOptions struct {
	HasDataType bool
	DataType    string // "SMALLINT", "INTEGER", or "BIGINT"

	HasIncrement bool
	Increment    int64

	HasMinValue bool
	NoMinValue  bool
	MinValue    int64

	HasMaxValue bool
	NoMaxValue  bool
	MaxValue    int64

	HasStart bool
	Start    int64

	HasRestart      bool
	HasRestartValue bool
	RestartValue    int64

	HasCache bool
	Cache    int64

	HasCycle bool
	Cycle    bool
}

// CreateSequenceStmt is one parsed CREATE SEQUENCE statement.
type CreateSequenceStmt struct {
	Name        string
	IfNotExists bool
	Options     SequenceOptions
	Pos         int
}

// AlterSequenceStmt is one parsed ALTER SEQUENCE statement.
type AlterSequenceStmt struct {
	Name     string
	IfExists bool
	Options  SequenceOptions
	Pos      int
}

// DropSequenceStmt is one parsed DROP SEQUENCE statement.
type DropSequenceStmt struct {
	Name     string
	IfExists bool
	Restrict bool
	Pos      int
}
