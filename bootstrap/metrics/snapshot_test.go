package metrics

import (
	"maps"
	"sync"
	"testing"

	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging/mocks"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
	gometrics "github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type snapshotTelemetryConfig struct {
	interfaces.Configuration
	mu        sync.RWMutex
	telemetry config.TelemetryInfo
}

func (c *snapshotTelemetryConfig) GetTelemetryInfo() *config.TelemetryInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := c.telemetry
	result.Metrics = maps.Clone(result.Metrics)
	result.Tags = maps.Clone(result.Tags)
	return &result
}
func (c *snapshotTelemetryConfig) set(enabled bool, site string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.telemetry = config.TelemetryInfo{Metrics: map[string]bool{"Queue": enabled}, Tags: map[string]string{"site": site}}
}

func TestReporterRefreshesTelemetrySnapshots(t *testing.T) {
	cfg := &snapshotTelemetryConfig{}
	cfg.set(false, "initial")
	client := &mocks.MessageClient{}
	var published []dtos.Metric
	client.On("Publish", mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		metric, err := types.GetMsgPayload[dtos.Metric](args.Get(0).(types.MessageEnvelope))
		require.NoError(t, err)
		published = append(published, metric)
	})
	dic := di.NewContainer(di.ServiceConstructorMap{
		container.ConfigurationInterfaceName: func(di.Get) any { return cfg },
		container.MessagingClientName:        func(di.Get) any { return client },
	})
	reporter := NewMessageBusReporter(logger.NewMockClient(), "edgex", "test", dic, cfg.GetTelemetryInfo())
	registry := gometrics.NewRegistry()
	require.NoError(t, registry.Register("Queue", gometrics.NewCounter()))
	cfg.set(true, "first")
	require.NoError(t, reporter.Report(registry, nil))
	require.Len(t, published, 1)
	require.Contains(t, published[0].Tags, dtos.MetricTag{Name: "site", Value: "first"})
	cfg.set(false, "disabled")
	require.NoError(t, reporter.Report(registry, nil))
	require.Len(t, published, 1)
	cfg.set(true, "second")
	require.NoError(t, reporter.Report(registry, nil))
	require.Len(t, published, 2)
	require.Contains(t, published[1].Tags, dtos.MetricTag{Name: "site", Value: "second"})

	// Updating settings while reports read detached snapshots must remain safe.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			cfg.set(i%2 == 0, "concurrent")
		}
	}()
	for i := 0; i < 500; i++ {
		require.NoError(t, reporter.Report(registry, nil))
	}
	wg.Wait()
}
