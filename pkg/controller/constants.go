package controller

import "time"

const (
	// PhasePending is the phase for a record that has been validated but not yet acted on.
	PhasePending = "Pending"

	// DefaultSyncInterval is the default interval for periodic resyncs in the allocation controller.
	DefaultSyncInterval = 30 * time.Second
)
