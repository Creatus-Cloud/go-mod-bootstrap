package config

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	bootstrapconfig "github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/edgexfoundry/go-mod-configuration/v4/configuration"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/stretchr/testify/require"
)

type commonReloadProvider struct {
	configuration.Client
	load    func(*snapshotReloadConfig)
	loads   int
	ready   chan (chan<- any)
	stopped bool
}

func (p *commonReloadProvider) GetConfiguration(raw any) (any, error) {
	p.loads++
	p.load(raw.(*snapshotReloadConfig))
	return raw, nil
}

func (p *commonReloadProvider) WatchForChanges(updates chan<- any, _ chan<- error, _ any, _ string, _ func() messaging.MessageClient) {
	p.ready <- updates
}

func (p *commonReloadProvider) StopWatching() {
	p.stopped = true
}

func TestCommonWritableWatcherReloadsAllChangesWithOverrides(t *testing.T) {
	for _, tc := range []struct{ source, override string }{
		{source: "all", override: "private"},
		{source: "all", override: "app"},
		{source: "all", override: "device"},
		{source: "app", override: "private"},
		{source: "device", override: "private"},
	} {
		for _, operation := range []string{"update", "delete"} {
			t.Run(tc.source+"/"+tc.override+"/"+operation, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					initial := WritableInfo{LogLevel: "WARN", Telemetry: bootstrapconfig.TelemetryInfo{
						Interval: "30s", Metrics: map[string]bool{"TestMetric": true},
					}}
					live := &snapshotReloadConfig{ConfigurationMockStruct: ConfigurationMockStruct{Writable: initial}}
					commonProvider := &commonReloadProvider{load: func(target *snapshotReloadConfig) {
						target.Writable = initial
						target.Writable.LogLevel = "INFO"
					}}
					overrideProvider := &commonReloadProvider{load: func(target *snapshotReloadConfig) {
						target.Writable.LogLevel = "WARN"
					}}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					cp := &Processor{ctx: ctx, wg: &sync.WaitGroup{}, lc: logger.NewMockClient(),
						commonConfigClient: commonProvider, configUpdated: make(UpdatedStream, 1),
					}
					source := commonProvider
					if tc.source != "all" {
						source = &commonReloadProvider{}
						if tc.source == "app" {
							cp.appConfigClient = source
						} else {
							cp.deviceConfigClient = source
						}
					}
					switch tc.override {
					case "private":
						cp.privateConfigClient = overrideProvider
					case "app":
						cp.appConfigClient = overrideProvider
					case "device":
						cp.deviceConfigClient = overrideProvider
					}
					source.ready = make(chan (chan<- any))
					source.load = func(target *snapshotReloadConfig) {
						target.Writable = initial
						target.Writable.LogLevel = "DEBUG"
						if operation == "delete" {
							target.Writable.Telemetry.Metrics = nil
						} else {
							target.Writable.Telemetry.Metrics = map[string]bool{"TestMetric": false}
						}
					}
					cp.listenForCommonChanges(live, source)
					updates := <-source.ready
					updates <- nil // Provider's connection announcement must preserve startup overrides.
					synctest.Wait()
					require.Zero(t, source.loads)
					require.Equal(t, "WARN", live.GetLogLevel())
					require.True(t, live.GetTelemetryInfo().Metrics["TestMetric"])

					// One notification contains both an overridden logging change and
					// an effective metric change/deletion. Neither may hide the other.
					updates <- &initial
					synctest.Wait()
					require.Equal(t, 1, commonProvider.loads)
					require.Equal(t, 1, overrideProvider.loads)
					require.Equal(t, 1, source.loads)
					require.Equal(t, "WARN", live.GetLogLevel())
					require.False(t, live.GetTelemetryInfo().Metrics["TestMetric"])
					_, exists := live.GetTelemetryInfo().Metrics["TestMetric"]
					require.Equal(t, operation == "update", exists)
					require.Len(t, cp.configUpdated, 1)
					cancel()
					cp.wg.Wait()
					require.True(t, source.stopped)
				})
			})
		}
	}
}
