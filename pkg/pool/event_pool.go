package pool

import (
	"sync"

	"github.com/foden/cdc/pkg/models"
)

var eventPool = sync.Pool{
	New: func() any {
		return new(models.Event)
	},
}

// GetEvent retrieves an Event from the pool or allocates a new one.
func GetEvent() *models.Event {
	return eventPool.Get().(*models.Event)
}

// PutEvent resets the Event fields and returns it to the pool.
func PutEvent(ev *models.Event) {
	if ev == nil {
		return
	}
	*ev = models.Event{}
	eventPool.Put(ev)
}
