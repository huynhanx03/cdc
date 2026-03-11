package registry

import (
	"fmt"

	"github.com/foden/cdc/pkg/config"
	"github.com/foden/cdc/pkg/interfaces"
)

// SourceFactory is a constructor function for creating a Source from config.
type SourceFactory func(cfg *config.SourceConfig) (interfaces.Source, error)

// SinkFactory is a constructor function for creating a Sink from config.
type SinkFactory func(cfg *config.SinkConfig) (interfaces.Sink, error)

var (
	sources = map[string]SourceFactory{}
	sinks   = map[string]SinkFactory{}
)

// RegisterSource registers a source factory under a given type name.
// Typically called from init() in each source package.
func RegisterSource(typeName string, factory SourceFactory) {
	if _, exists := sources[typeName]; exists {
		panic(fmt.Sprintf("source type %q already registered", typeName))
	}
	sources[typeName] = factory
}

// RegisterSink registers a sink factory under a given type name.
// Typically called from init() in each sink package.
func RegisterSink(typeName string, factory SinkFactory) {
	if _, exists := sinks[typeName]; exists {
		panic(fmt.Sprintf("sink type %q already registered", typeName))
	}
	sinks[typeName] = factory
}

// CreateSource looks up a registered source factory and creates a Source.
func CreateSource(cfg *config.SourceConfig) (interfaces.Source, error) {
	factory, ok := sources[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported source type: %q (registered: %v)", cfg.Type, SourceNames())
	}
	return factory(cfg)
}

// CreateSink looks up a registered sink factory and creates a Sink.
func CreateSink(cfg *config.SinkConfig) (interfaces.Sink, error) {
	factory, ok := sinks[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported sink type: %q (registered: %v)", cfg.Type, SinkNames())
	}
	return factory(cfg)
}

func SourceNames() []string {
	names := make([]string, 0, len(sources))
	for k := range sources {
		names = append(names, k)
	}
	return names
}

func SinkNames() []string {
	names := make([]string, 0, len(sinks))
	for k := range sinks {
		names = append(names, k)
	}
	return names
}
