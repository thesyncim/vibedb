package pgwire

// The wire vocabulary: message tags, the fixed protocol integers, the type
// OIDs this server declares, and the limits every decode is bounded by.
//
// Everything here is transcribed from the PostgreSQL frontend/backend protocol
// specification (protocol version 3.0) rather than lifted from a client
// library, so each constant is named for the message it belongs to and the
// direction it travels. A tag byte is unique only within a direction: 'D' is
// Describe from the frontend and DataRow from the backend, and 'C' is Close
// from the frontend and CommandComplete from the backend. Splitting the two
// sets is what keeps that from becoming a silent bug the day a dispatch switch
// is copied from one side to the other.

// Frontend message tags: what a client sends after the startup packet.
const (
	msgBind         = 'B'
	msgClose        = 'C'
	msgCopyData     = 'd'
	msgCopyDone     = 'c'
	msgCopyFail     = 'f'
	msgDescribe     = 'D'
	msgExecute      = 'E'
	msgFlush        = 'H'
	msgFunctionCall = 'F'
	msgParse        = 'P'
	// msgPasswordOrSASL is 'p' for every authentication reply the frontend
	// sends: cleartext password, SASLInitialResponse, SASLResponse, and the GSS
	// replies this server does not implement. The overload is the protocol's,
	// and it is why the authentication exchange reads its replies by tag rather
	// than by name.
	msgPasswordOrSASL = 'p'
	msgQuery          = 'Q'
	msgSync           = 'S'
	msgTerminate      = 'X'
)

// Backend message tags: what this server sends.
const (
	msgAuthentication  = 'R'
	msgBackendKeyData  = 'K'
	msgBindComplete    = '2'
	msgCloseComplete   = '3'
	msgCommandComplete = 'C'
	msgDataRow         = 'D'
	msgEmptyQuery      = 'I'
	msgErrorResponse   = 'E'
	msgNoData          = 'n'
	msgNoticeResponse  = 'N'
	msgParameterDesc   = 't'
	msgParameterStatus = 'S'
	msgParseComplete   = '1'
	msgPortalSuspended = 's'
	msgReadyForQuery   = 'Z'
	msgRowDescription  = 'T'
)

// Authentication request subtypes, the int32 that follows the 'R' tag.
const (
	authOK                = 0
	authCleartextPassword = 3
	authSASL              = 10
	authSASLContinue      = 11
	authSASLFinal         = 12
)

// Describe and Close operate on either a prepared statement or a portal, and
// say which with one byte.
const (
	targetStatement = 'S'
	targetPortal    = 'P'
)

// The startup packet has no tag byte. Its first int32 is a length and its
// second is either a protocol version or one of these request codes, which are
// deliberately chosen to be impossible version numbers.
const (
	protocolVersion30 = 196608 // 3<<16 | 0
	codeCancelRequest = 80877102
	codeSSLRequest    = 80877103
	codeGSSENCRequest = 80877104
)

// Format codes name the encoding of a parameter or a result column.
const (
	formatText   = 0
	formatBinary = 1
)

// Transaction status, the byte ReadyForQuery carries.
//
// This server sends only [statusIdle] today. 'T' would claim an open
// transaction block and 'E' a failed one, and there is no transaction block
// here to be in: BEGIN is refused rather than accepted as a no-op, so a client
// can never reach a state where either byte would be the truth. The other two
// are defined because the byte's meaning is part of the protocol contract and a
// reader of this file should not have to go looking for what the server is
// choosing not to say.
const (
	statusIdle    = 'I' // not in a transaction block
	statusInTx    = 'T' // in a transaction block
	statusFailedT = 'E' // in a failed transaction block
)

// PostgreSQL type OIDs this server declares in RowDescription. See rows.go for
// why the schemaless projection is json and not text, numeric, or jsonb.
const (
	oidBool    = 16
	oidInt8    = 20
	oidText    = 25
	oidJSON    = 114
	oidJSONB   = 3802
	oidNumeric = 1700
)

// Limits. Every one of these bounds an allocation that a remote peer would
// otherwise get to choose the size of; see reader.go for the reasoning.
const (
	// maxMessageBody is the largest frontend message body this server will
	// read. It is generous enough for a very large statement or bound
	// parameter and small enough that a connection cannot be used to ask for
	// an arbitrary allocation.
	maxMessageBody = 16 << 20

	// maxStartupPacket matches PostgreSQL's own MAX_STARTUP_PACKET_LENGTH. The
	// startup packet arrives before any authentication has happened, so it is
	// the one message an entirely unknown peer can make the server allocate
	// for, and it is bounded far more tightly than the rest.
	maxStartupPacket = 10000

	// retainedBuffer caps the read buffer a session keeps between messages. A
	// body larger than this is read into storage that is dropped as soon as the
	// message is handled, so one oversized statement does not leave every idle
	// connection holding megabytes.
	retainedBuffer = 1 << 16

	// maxIdentifier bounds a prepared-statement or portal name. PostgreSQL's
	// own NAMEDATALEN is 64; this is loose enough never to reject a real client
	// and tight enough that names cannot be used as a memory sink, since a
	// session retains one per open statement.
	maxIdentifier = 256

	// maxStatements and maxPortals bound the per-session object tables. Both
	// grow only on explicit client request and shrink on Close, so without a
	// cap a client could name a million statements and hold their plans.
	maxStatements = 1024
	maxPortals    = 1024

	// maxParameters bounds the parameter and format-code counts a Bind may
	// declare. The wire field is a signed 16-bit integer, so 32767 is the
	// protocol's own ceiling and not a policy of this server's: a larger count
	// cannot be expressed. It is stated explicitly rather than left implicit in
	// a conversion, because leaving it implicit is how a decoder ends up with a
	// negative length it treats as zero.
	maxParameters = 32767
)
