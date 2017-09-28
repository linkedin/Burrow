package helpers

import (
	"github.com/linkedin/Burrow/core/protocol"
)

func StartCoordinatorModules(modules map[string]protocol.Module) error {
	// Start all the modules, returning an error if any fail to start
	for _, module := range modules {
		err := module.Start()
		if err != nil {
			return err
		}
	}
	return nil
}

func StopCoordinatorModules(modules map[string]protocol.Module) {
	// Stop all the modules passed in
	for _, module := range modules {
		module.Stop()
	}
}
