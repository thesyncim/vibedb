package main

import (
	"errors"
	"path/filepath"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

// rf3SchemaControlServices retains the node-wide schema journal and artifacts.
// The activator and transport registry resolve local groups at request time,
// including groups enrolled after an initially empty node starts serving.
type rf3SchemaControlServices struct {
	install   *schemainstall.ControlService
	build     *schemainstall.BuildControlService
	journal   *schemainstall.FileJournal
	artifacts *schemainstall.DirectoryBackend
}

func newRF3SchemaControlServices(
	activator *rf3SchemaActivator,
	registry *rafttransport.StaticRegistry,
	policy *serviceauthz.Policy,
	root string,
) (_ *rf3SchemaControlServices, resultErr error) {
	if activator == nil || registry == nil || policy == nil || root == "" {
		return nil, errRF3Serving
	}
	services := &rf3SchemaControlServices{}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, services.Close())
		}
	}()
	var err error
	services.journal, err = schemainstall.OpenFileJournal(
		filepath.Join(root, "schema-rollout-journal"), rf3SchemaInstallRecords,
	)
	if err != nil {
		return nil, err
	}
	services.artifacts, err = schemainstall.OpenDirectoryBackend(schemainstall.DirectoryOptions{
		Path:         filepath.Join(root, "schema-rollout-artifacts"),
		MaxArtifacts: rf3SchemaInstallArtifacts, MaxDiskBytes: rf3SchemaInstallDiskBytes,
		Activator: activator,
	})
	if err != nil {
		return nil, err
	}
	if err = activator.bindArtifacts(services.artifacts); err != nil {
		return nil, err
	}
	installer, err := schemainstall.New(schemainstall.Options{
		Journal: services.journal, Backend: services.artifacts, MaxConcurrent: 8,
	})
	if err != nil {
		return nil, err
	}
	deadline := servicetls.FixedDeadline(2 * time.Minute)
	services.install, err = schemainstall.NewControlService(schemainstall.ControlOptions{
		Installer: installer,
		Authorize: func(identity rafttransport.PeerIdentity, request schemainstall.Request, _ schemainstall.Command) bool {
			_, err := registry.LocalMember(request.Group)
			return err == nil && policy.Check(identity.Node, serviceauthz.CapabilitySchema) == serviceauthz.DecisionAllow
		},
		ReadDeadline: deadline, WriteDeadline: deadline,
		MaxBundleBytes: schemainstall.AbsoluteMaxBundleBytes,
	})
	if err != nil {
		return nil, err
	}
	services.build, err = schemainstall.NewBuildControlService(schemainstall.BuildControlOptions{
		Builder: activator,
		Authorize: func(identity rafttransport.PeerIdentity, request schemainstall.BuildRequest) bool {
			_, err := registry.LocalMember(request.Group)
			return err == nil && policy.Check(identity.Node, serviceauthz.CapabilitySchema) == serviceauthz.DecisionAllow
		},
		ReadDeadline: deadline, WriteDeadline: deadline, MaxConcurrent: 2, BuildTimeout: 2 * time.Minute,
	})
	if err != nil {
		return nil, err
	}
	return services, nil
}

func (services *rf3SchemaControlServices) Close() error {
	if services == nil {
		return nil
	}
	return errors.Join(services.artifacts.Close(), services.journal.Close())
}
