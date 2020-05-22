package common

import (
	"context"
	"sync"
)

type ForwarderBackendConfig struct {
	// for file metadata persistence.
	// leave empty to disable persistence.
	// if persistence is disabled, all file stats will be discarded when filesystem is umounted or the mount process exits.
	PersistentFile string
	WG             *sync.WaitGroup
	CTX            context.Context
}
