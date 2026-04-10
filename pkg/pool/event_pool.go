package pool

import (
	"sync"

	"github.com/foden/cdc/pkg/models"
)

var eventPool = sync.Pool{
	New: func() any {
		return &models.Event{}
	},
}

// GetEvent retrieves an Event from the pool or allocates a new one.
func GetEvent() *models.Event {
	return eventPool.Get().(*models.Event)
}

// PutEvent resets the Event fields and returns it to the pool.
func PutEvent(ev *models.Event) {
	ev.Topic = ""
	ev.Subject = ""
	ev.InstanceID = ""
	ev.Schema = ""
	ev.Table = ""
	ev.Op = ""
	ev.Offset = ""
	ev.LSN = 0
	ev.Data = nil
	eventPool.Put(ev)
}
