package schemainstall

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	requestMagic  = [8]byte{'V', 'B', 'S', 'C', 'R', 'E', 'Q', 0}
	responseMagic = [8]byte{'V', 'B', 'S', 'C', 'R', 'E', 'S', 0}
)

const (
	ControlRequestBytes  = 592
	ControlResponseBytes = 652
)

type Command uint8

const (
	CommandPrepare Command = iota + 1
	CommandAuthorize
	CommandActivate
	CommandDrain
)

func (command Command) valid() bool { return command >= CommandPrepare && command <= CommandDrain }

type ResponseCode uint8

const (
	ResponseOK ResponseCode = iota
	ResponseInvalid
	ResponseUnauthorized
	ResponseConflict
	ResponseMissing
	ResponseBound
	ResponseOutcomeUnknown
	ResponseInternal
)

func RequestDiscriminator() [8]byte { return requestMagic }

type AuthorizeFunc func(rafttransport.PeerIdentity, Request, Command) bool

type ControlOptions struct {
	Installer      *Installer
	Authorize      AuthorizeFunc
	ReadDeadline   rafttransport.DeadlineFunc
	WriteDeadline  rafttransport.DeadlineFunc
	MaxBundleBytes uint64
}

// ControlService executes exactly one authenticated command per connection.
// TLS authenticates the receipt's serving node; the response binds the exact
// durable record, so a gateway never treats transport success as installation.
type ControlService struct {
	installer      *Installer
	authorize      AuthorizeFunc
	readDeadline   rafttransport.DeadlineFunc
	writeDeadline  rafttransport.DeadlineFunc
	maxBundleBytes uint64
}

func NewControlService(options ControlOptions) (*ControlService, error) {
	if options.Installer == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxBundleBytes == 0 ||
		options.MaxBundleBytes > AbsoluteMaxBundleBytes {
		return nil, ErrInvalid
	}
	return &ControlService{installer: options.Installer, authorize: options.Authorize,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		maxBundleBytes: options.MaxBundleBytes}, nil
}

func (service *ControlService) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrInvalid
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := service.readDeadline(); deadline.IsZero() {
		return ErrInvalid
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	command, request, authorization, proof, err := ReadControlRequest(connection)
	if err != nil {
		return err
	}
	peer := connection.PeerIdentity()
	domain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID,
		ClusterIncarnation: request.Group.ClusterIncarnation}
	if peer.TrustDomain != domain || !service.authorize(peer, request, command) {
		return service.writeResponse(connection, ResponseUnauthorized, Record{})
	}
	var record Record
	switch command {
	case CommandPrepare:
		if request.BundleBytes > service.maxBundleBytes {
			err = ErrBound
			break
		}
		bundle := make([]byte, int(request.BundleBytes))
		if _, err = io.ReadFull(connection, bundle); err == nil {
			_, err = service.installer.Prepare(ctx, request, bundle)
			if err == nil {
				record, err = service.installer.Read(ctx, request.Operation)
			}
		}
		clear(bundle)
	case CommandAuthorize:
		record, err = service.installer.Authorize(ctx, authorization)
	case CommandActivate:
		record, err = service.installer.Activate(ctx, authorization)
	case CommandDrain:
		record, err = service.installer.Drain(ctx, authorization, proof)
	default:
		err = ErrInvalid
	}
	code := responseCode(err)
	if deadline := service.writeDeadline(); deadline.IsZero() {
		return ErrInvalid
	} else if deadlineErr := connection.SetWriteDeadline(deadline); deadlineErr != nil {
		return deadlineErr
	}
	if writeErr := service.writeResponse(connection, code, record); writeErr != nil {
		return writeErr
	}
	return err
}

func (service *ControlService) writeResponse(connection io.Writer, code ResponseCode, record Record) error {
	var raw [ControlResponseBytes]byte
	copy(raw[:8], responseMagic[:])
	raw[8] = byte(code)
	if code == ResponseOK {
		encoded, err := appendRecord(raw[:16], record)
		if err != nil || len(encoded) != len(raw) {
			return errors.Join(ErrInvalid, err)
		}
		copy(raw[:], encoded)
	}
	return writeAll(connection, raw[:])
}

func AppendControlRequest(dst []byte, command Command, request Request, authorization Authorization, proof DrainProof) ([]byte, error) {
	if !command.valid() || !validRequest(request) ||
		(command == CommandPrepare && authorization != (Authorization{}) ||
			command != CommandPrepare && !validAuthorization(authorization, request.Operation)) ||
		(command == CommandDrain && !validDrainProof(proof, authorization) || command != CommandDrain && proof != (DrainProof{})) {
		return dst, ErrInvalid
	}
	start := len(dst)
	dst = append(dst, make([]byte, ControlRequestBytes)...)
	raw := dst[start:]
	copy(raw[:8], requestMagic[:])
	raw[8] = byte(command)
	at := putRequest(raw, 16, request)
	at = putAuthorization(raw, at, authorization)
	at = putDrainProof(raw, at, proof)
	if at != len(raw) {
		return dst[:start], ErrInvalid
	}
	return dst, nil
}

func ReadControlRequest(reader io.Reader) (Command, Request, Authorization, DrainProof, error) {
	var raw [ControlRequestBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return 0, Request{}, Authorization{}, DrainProof{}, err
	}
	command := Command(raw[8])
	if !bytes.Equal(raw[:8], requestMagic[:]) || !command.valid() ||
		binary.LittleEndian.Uint64(raw[8:16]) != uint64(command) {
		return 0, Request{}, Authorization{}, DrainProof{}, ErrInvalid
	}
	request, at := getRequest(raw[:], 16)
	authorization, at := getAuthorization(raw[:], at)
	proof, at := getDrainProof(raw[:], at)
	if at != len(raw) || !validRequest(request) ||
		(command == CommandPrepare && authorization != (Authorization{}) ||
			command != CommandPrepare && !validAuthorization(authorization, request.Operation)) ||
		(command == CommandDrain && !validDrainProof(proof, authorization) || command != CommandDrain && proof != (DrainProof{})) {
		return 0, Request{}, Authorization{}, DrainProof{}, ErrInvalid
	}
	canonical, err := AppendControlRequest(nil, command, request, authorization, proof)
	if err != nil || !bytes.Equal(canonical, raw[:]) {
		return 0, Request{}, Authorization{}, DrainProof{}, ErrInvalid
	}
	return command, request, authorization, proof, nil
}

func ReadControlResponse(reader io.Reader) (Record, error) {
	var raw [ControlResponseBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return Record{}, err
	}
	if !bytes.Equal(raw[:8], responseMagic[:]) || binary.LittleEndian.Uint64(raw[8:16]) != uint64(raw[8]) {
		return Record{}, ErrInvalid
	}
	code := ResponseCode(raw[8])
	if code != ResponseOK {
		for _, value := range raw[16:] {
			if value != 0 {
				return Record{}, ErrInvalid
			}
		}
		return Record{}, responseError(code)
	}
	return openRecord(raw[16:])
}

func responseCode(err error) ResponseCode {
	switch {
	case err == nil:
		return ResponseOK
	case errors.Is(err, ErrConflict):
		return ResponseConflict
	case errors.Is(err, ErrMissing):
		return ResponseMissing
	case errors.Is(err, ErrBound):
		return ResponseBound
	case errors.Is(err, ErrOutcomeUnknown):
		return ResponseOutcomeUnknown
	case errors.Is(err, ErrInvalid):
		return ResponseInvalid
	default:
		return ResponseInternal
	}
}

func responseError(code ResponseCode) error {
	switch code {
	case ResponseInvalid:
		return ErrInvalid
	case ResponseUnauthorized:
		return rafttransport.ErrUnauthorized
	case ResponseConflict:
		return ErrConflict
	case ResponseMissing:
		return ErrMissing
	case ResponseBound:
		return ErrBound
	case ResponseOutcomeUnknown:
		return ErrOutcomeUnknown
	case ResponseInternal:
		return errors.New("schemainstall: remote internal failure")
	default:
		return ErrInvalid
	}
}

var _ interface {
	Serve(context.Context, rafttransport.PeerConnection) error
} = (*ControlService)(nil)
