package schemainstall

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

type SchemaBuilder interface {
	BuildSchema(context.Context, BuildRequest, string) (sqldriver.ReplicatedSchemaDDLTarget, error)
}

type schemaBuildResumer interface {
	ResumeSchemaBuild(context.Context, [32]byte, raftmember.GroupKey) (BuildRequest, string, sqldriver.ReplicatedSchemaDDLTarget, bool, error)
}

type schemaShadowBuilder interface {
	BuildSchemaShadow(context.Context, BuildRequest, string) (bool, error)
}

type BuildControlOptions struct {
	Builder                     SchemaBuilder
	Authorize                   func(rafttransport.PeerIdentity, BuildRequest) bool
	ReadDeadline, WriteDeadline rafttransport.DeadlineFunc
	MaxConcurrent               int
	BuildTimeout                time.Duration
}

type BuildControlService struct {
	options  BuildControlOptions
	admitted chan struct{}
}

func NewBuildControlService(options BuildControlOptions) (*BuildControlService, error) {
	if options.Builder == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent < 1 || options.MaxConcurrent > 64 ||
		options.BuildTimeout <= 0 || options.BuildTimeout > time.Hour {
		return nil, ErrInvalid
	}
	return &BuildControlService{options: options, admitted: make(chan struct{}, options.MaxConcurrent)}, nil
}

func (s *BuildControlService) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if s == nil || ctx == nil || connection == nil || connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrInvalid
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	deadline := boundedClientDeadline(ctx, s.options.ReadDeadline())
	if deadline.IsZero() {
		return ErrInvalid
	}
	if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	r, err := ReadBuildRequest(connection)
	if err != nil {
		return err
	}
	domain := rafttransport.TrustDomain{ClusterID: r.Group.ClusterID, ClusterIncarnation: r.Group.ClusterIncarnation}
	if connection.PeerIdentity().TrustDomain != domain || !s.options.Authorize(connection.PeerIdentity(), r) {
		return s.writeResponse(ctx, connection, r, ResponseUnauthorized, nil)
	}
	select {
	case s.admitted <- struct{}{}:
		defer func() { <-s.admitted }()
	default:
		return s.writeResponse(ctx, connection, r, ResponseBound, nil)
	}
	if err := s.writeResponse(ctx, connection, r, ResponseOK, nil); err != nil {
		return err
	}
	if r.Resume {
		resumer, ok := s.options.Builder.(schemaBuildResumer)
		if !ok {
			return s.writeResponse(ctx, connection, r, ResponseInvalid, nil)
		}
		request, sql, target, active, resumeErr := resumer.ResumeSchemaBuild(ctx, r.Operation, r.Group)
		var body []byte
		if resumeErr == nil {
			body, resumeErr = appendBuildResumeReceipt(nil, request, sql, target, active)
		}
		if writeErr := s.writeResponse(ctx, connection, r, responseCode(resumeErr), body); writeErr != nil {
			return writeErr
		}
		return resumeErr
	}
	// Authorization and admission precede even this bounded SQL allocation.
	sql := make([]byte, int(r.SQLBytes))
	if _, err := io.ReadFull(connection, sql); err != nil {
		return err
	}
	if sha256.Sum256(sql) != r.SQLDigest {
		return s.writeResponse(ctx, connection, r, ResponseInvalid, nil)
	}
	// A disconnected caller must not leave unbounded cold work running. The
	// reserved build identity survives deadline cancellation for exact retry.
	buildCtx, cancel := context.WithTimeout(ctx, s.options.BuildTimeout)
	defer cancel()
	if r.Shadow {
		builder, ok := s.options.Builder.(schemaShadowBuilder)
		if !ok {
			return s.writeResponse(ctx, connection, r, ResponseInvalid, nil)
		}
		noOp, shadowErr := builder.BuildSchemaShadow(buildCtx, r, string(sql))
		var body []byte
		if shadowErr == nil {
			body = []byte{0}
			if noOp {
				body[0] = 1
			}
		}
		if writeErr := s.writeResponse(ctx, connection, r, responseCode(shadowErr), body); writeErr != nil {
			return writeErr
		}
		return shadowErr
	}
	target, err := s.options.Builder.BuildSchema(buildCtx, r, string(sql))
	var body []byte
	if err == nil {
		body, err = appendBuildReceipt(target, r)
	}
	code := responseCode(err)
	switch {
	case errors.Is(err, durable.ErrCommitOutcomeUnknown):
		code = ResponseOutcomeUnknown
	case errors.Is(err, sqldriver.ErrReplicatedSchemaDDLConflict), errors.Is(err, sqldriver.ErrTransactionConflict):
		code = ResponseConflict
	case errors.Is(err, sqldriver.ErrReplicatedSchemaCatalogImage):
		code = ResponseInvalid
	}
	if err != nil {
		body = nil
	}
	if writeErr := s.writeResponse(ctx, connection, r, code, body); writeErr != nil {
		return writeErr
	}
	return err
}

func (s *BuildControlService) writeResponse(ctx context.Context, connection rafttransport.PeerConnection, request BuildRequest, code ResponseCode, body []byte) error {
	deadline := boundedClientDeadline(ctx, s.options.WriteDeadline())
	if deadline.IsZero() {
		return ErrInvalid
	}
	if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	var header [buildResponseBytes]byte
	copy(header[:8], buildResponseMagic[:])
	header[8] = byte(code)
	digest, err := BuildRequestDigest(request)
	if err != nil {
		return err
	}
	copy(header[16:48], digest[:])
	binary.LittleEndian.PutUint64(header[48:56], uint64(len(body)))
	if err := writeAll(connection, header[:]); err != nil {
		return err
	}
	return writeAll(connection, body)
}

// Build materializes an unpublished successor on one authenticated replica.
// Transport errors retain uncertainty: retry this exact request, never mint
// a replacement operation or infer that a timed-out build did not run.
func (c *Client) Build(ctx context.Context, node rafttransport.NodeID, request BuildRequest, sql string) (sqldriver.ReplicatedSchemaDDLTarget, error) {
	var target sqldriver.ReplicatedSchemaDDLTarget
	if c == nil || ctx == nil || node == (rafttransport.NodeID{}) || request.Shadow || !validBuildRequest(request) ||
		uint64(len(sql)) != request.SQLBytes || sha256.Sum256([]byte(sql)) != request.SQLDigest {
		return target, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return target, err
	}
	connection, err := c.opener.OpenShardControl(ctx, node)
	if err != nil || connection == nil {
		if connection != nil {
			_ = connection.Close()
		}
		return target, errors.Join(ErrMissing, err)
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}
	if connection.TrafficClass() != rafttransport.TrafficShardControl || peer.Node != node || peer.TrustDomain != domain {
		return target, rafttransport.ErrUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	deadline := boundedClientDeadline(ctx, c.writeDeadline())
	if deadline.IsZero() {
		return target, ErrInvalid
	}
	if err := connection.SetWriteDeadline(deadline); err != nil {
		return target, err
	}
	raw, err := AppendBuildRequest(nil, request)
	if err != nil {
		return target, err
	}
	// Admission precedes the SQL body. It is not a build receipt and cannot
	// authorize prepare; it prevents full-duplex deadlock on early rejection.
	if err = writeAll(connection, raw); err != nil {
		return target, err
	}
	deadline = boundedClientDeadline(ctx, c.readDeadline())
	if deadline.IsZero() {
		return target, ErrInvalid
	}
	if err := connection.SetReadDeadline(deadline); err != nil {
		return target, err
	}
	if n, err := readBuildResponseHeader(connection, request); err != nil || n != 0 {
		return target, errors.Join(ErrInvalid, err)
	}
	if err := writeAll(connection, []byte(sql)); err != nil {
		return target, errors.Join(ErrOutcomeUnknown, err)
	}
	target, err = readBuildResponse(connection, request)
	if err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrBound) || errors.Is(err, rafttransport.ErrUnauthorized) {
			return target, err
		}
		return target, errors.Join(ErrOutcomeUnknown, err)
	}
	return target, nil
}

// BuildShadow starts or resumes an online snapshot copy and change capture on
// one replica. Success is not an activation receipt; the ordinary Build RPC
// finalizes it at the later exact fenced cut.
func (c *Client) BuildShadow(ctx context.Context, node rafttransport.NodeID, request BuildRequest, sql string) (bool, error) {
	if c == nil || ctx == nil || node == (rafttransport.NodeID{}) || !request.Shadow ||
		!validBuildRequest(request) || uint64(len(sql)) != request.SQLBytes ||
		sha256.Sum256([]byte(sql)) != request.SQLDigest {
		return false, ErrInvalid
	}
	connection, err := c.opener.OpenShardControl(ctx, node)
	if err != nil || connection == nil {
		if connection != nil {
			_ = connection.Close()
		}
		return false, errors.Join(ErrMissing, err)
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}
	if connection.TrafficClass() != rafttransport.TrafficShardControl || peer.Node != node || peer.TrustDomain != domain {
		return false, rafttransport.ErrUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := boundedClientDeadline(ctx, c.writeDeadline()); deadline.IsZero() {
		return false, ErrInvalid
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return false, err
	}
	raw, err := AppendBuildRequest(nil, request)
	if err != nil {
		return false, err
	}
	if err = writeAll(connection, raw); err != nil {
		return false, err
	}
	if deadline := boundedClientDeadline(ctx, c.readDeadline()); deadline.IsZero() {
		return false, ErrInvalid
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return false, err
	}
	if n, err := readBuildResponseHeader(connection, request); err != nil || n != 0 {
		return false, errors.Join(ErrInvalid, err)
	}
	if err := writeAll(connection, []byte(sql)); err != nil {
		return false, errors.Join(ErrOutcomeUnknown, err)
	}
	n, err := readBuildResponseHeader(connection, request)
	if err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrBound) || errors.Is(err, rafttransport.ErrUnauthorized) {
			return false, err
		}
		return false, errors.Join(ErrOutcomeUnknown, err)
	}
	if n != 1 {
		return false, ErrInvalid
	}
	var body [1]byte
	if _, err := io.ReadFull(connection, body[:]); err != nil || body[0] > 1 {
		return false, errors.Join(ErrOutcomeUnknown, err)
	}
	return body[0] == 1, nil
}

// ResumeBuild reads the exact retained build request, SQL and target receipt.
// It performs no materialization and is used only after coordinator restart.
func (c *Client) ResumeBuild(ctx context.Context, node rafttransport.NodeID,
	operation [32]byte, group raftmember.GroupKey,
) (BuildRequest, string, sqldriver.ReplicatedSchemaDDLTarget, bool, error) {
	lookup := BuildRequest{Resume: true, Operation: operation, Group: group}
	var target sqldriver.ReplicatedSchemaDDLTarget
	if c == nil || ctx == nil || node == (rafttransport.NodeID{}) || !validBuildRequest(lookup) {
		return BuildRequest{}, "", target, false, ErrInvalid
	}
	connection, err := c.opener.OpenShardControl(ctx, node)
	if err != nil || connection == nil {
		if connection != nil {
			_ = connection.Close()
		}
		return BuildRequest{}, "", target, false, errors.Join(ErrMissing, err)
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	domain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	if connection.TrafficClass() != rafttransport.TrafficShardControl || peer.Node != node || peer.TrustDomain != domain {
		return BuildRequest{}, "", target, false, rafttransport.ErrUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := boundedClientDeadline(ctx, c.writeDeadline()); deadline.IsZero() {
		return BuildRequest{}, "", target, false, ErrInvalid
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return BuildRequest{}, "", target, false, err
	}
	raw, err := AppendBuildRequest(nil, lookup)
	if err == nil {
		err = writeAll(connection, raw)
	}
	if err != nil {
		return BuildRequest{}, "", target, false, err
	}
	if deadline := boundedClientDeadline(ctx, c.readDeadline()); deadline.IsZero() {
		return BuildRequest{}, "", target, false, ErrInvalid
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return BuildRequest{}, "", target, false, err
	}
	if n, admissionErr := readBuildResponseHeader(connection, lookup); admissionErr != nil || n != 0 {
		return BuildRequest{}, "", target, false, errors.Join(admissionErr, ErrInvalid)
	}
	return readBuildResumeResponse(connection, lookup)
}
