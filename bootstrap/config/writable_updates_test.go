package config

import (
	"context"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces"
	bootstrapconfig "github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/edgexfoundry/go-mod-configuration/v4/configuration"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/stretchr/testify/require"
)

func TestWritableNotificationStopsOnCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		initial := WritableInfo{LogLevel: "INFO", Telemetry: bootstrapconfig.TelemetryInfo{Interval: "30s"}}
		live := &snapshotReloadConfig{ConfigurationMockStruct: ConfigurationMockStruct{Writable: initial}}
		loads := 0
		cp := &Processor{ctx: ctx, lc: logger.NewMockClient(), configUpdated: make(UpdatedStream),
			commonConfigClient: snapshotConfigProvider{load: func(target *snapshotReloadConfig) (any, error) {
				loads++
				target.Writable = initial
				return target, nil
			}},
		}
		var wg sync.WaitGroup
		wg.Go(func() { cp.applyWritableUpdates(live) })
		synctest.Wait() // The first watcher is blocked sending to an absent receiver.
		require.Equal(t, 1, loads)
		wg.Go(func() { cp.applyWritableUpdates(live) })
		cancel()
		wg.Wait() // Both watchers must stop without anyone receiving the signal.
		require.Equal(t, 1, loads, "canceled watcher must not reload provider data")
	})
}

type writableReloadConfig struct {
	ConfigurationMockStruct
	secrets bootstrapconfig.InsecureSecrets
}

type writableReloadInfo struct {
	WritableInfo
	Secrets bootstrapconfig.InsecureSecrets
}

func (c *writableReloadConfig) UpdateFromRaw(raw any) bool {
	next, ok := raw.(*writableReloadConfig)
	if ok {
		*c = *next
	}
	return ok
}

func (c *writableReloadConfig) GetWritablePtr() any {
	return &writableReloadInfo{WritableInfo: c.Writable, Secrets: c.secrets}
}

func (c *writableReloadConfig) UpdateWritableFromRaw(raw any) bool {
	next, ok := raw.(*writableReloadInfo)
	if ok {
		c.Writable, c.secrets = next.WritableInfo, next.Secrets
	}
	return ok
}

func (c *writableReloadConfig) GetInsecureSecrets() bootstrapconfig.InsecureSecrets {
	return c.secrets
}

type writableReloadProvider struct {
	configuration.Client
	next *writableReloadConfig
}

func (p writableReloadProvider) GetConfiguration(raw any) (any, error) {
	target := raw.(*writableReloadConfig)
	*target = *p.next
	return target, nil
}

type reloadMetricsManager struct {
	interfaces.MetricsManager
	intervals []time.Duration
}

func (m *reloadMetricsManager) ResetInterval(value time.Duration) {
	m.intervals = append(m.intervals, value)
}

type reloadSecretProvider struct {
	interfaces.SecretProviderExt
	names []string
}

func (s *reloadSecretProvider) SecretUpdatedAtSecretName(name string) {
	s.names = append(s.names, name)
}

type reloadLogger struct {
	logger.LoggingClient
	levels []string
}

func (l *reloadLogger) SetLogLevel(level string) error {
	l.levels = append(l.levels, level)
	return nil
}

func TestWritableReloadAppliesAllChangedSettings(t *testing.T) {
	for _, removeSecrets := range []bool{false, true} {
		name := "update secrets"
		if removeSecrets {
			name = "remove all secrets"
		}
		t.Run(name, func(t *testing.T) {
			live := &writableReloadConfig{
				ConfigurationMockStruct: ConfigurationMockStruct{Writable: WritableInfo{
					LogLevel: "INFO", Telemetry: bootstrapconfig.TelemetryInfo{Interval: "30s"},
				}},
				secrets: bootstrapconfig.InsecureSecrets{"mqtt": {SecretName: "mqtt", SecretData: map[string]string{"password": "old"}}},
			}
			next := &writableReloadConfig{
				ConfigurationMockStruct: ConfigurationMockStruct{Writable: WritableInfo{
					LogLevel: "DEBUG", Telemetry: bootstrapconfig.TelemetryInfo{Interval: "1s"},
					StoreAndForward: StoreAndForwardInfo{Enabled: true},
				}},
				secrets: bootstrapconfig.InsecureSecrets{"mqtt": {SecretName: "mqtt", SecretData: map[string]string{"password": "new"}}},
			}
			if removeSecrets {
				next.secrets = nil
			}
			manager := &reloadMetricsManager{}
			secrets := &reloadSecretProvider{}
			log := &reloadLogger{LoggingClient: logger.NewMockClient()}
			notifications := make(UpdatedStream, 2)
			cp := &Processor{ctx: context.Background(), lc: log, configUpdated: notifications,
				dic: di.NewContainer(di.ServiceConstructorMap{
					container.MetricsManagerInterfaceName: func(di.Get) any { return manager },
					container.SecretProviderExtName:       func(di.Get) any { return secrets },
				}),
				commonConfigClient: writableReloadProvider{next: next},
			}
			cp.applyWritableUpdates(live)
			require.Equal(t, []string{"DEBUG"}, log.levels)
			require.Equal(t, []string{"mqtt"}, secrets.names)
			require.Equal(t, []time.Duration{time.Second}, manager.intervals)
			require.True(t, live.Writable.StoreAndForward.Enabled)
			require.Len(t, notifications, 1, "service must receive one signal for the complete reload")
		})
	}
}

func TestWritableTelemetryChangeStillNotifiesService(t *testing.T) {
	for _, tc := range []struct {
		name       string
		interval   string
		noManager  bool
		wantResets []time.Duration
	}{
		{name: "invalid", interval: "invalid"},
		{name: "negative", interval: "-1s"},
		{name: "disabled", interval: "0s", wantResets: []time.Duration{math.MaxInt64}},
		{name: "missing manager", interval: "1s", noManager: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live := &snapshotReloadConfig{ConfigurationMockStruct: ConfigurationMockStruct{Writable: WritableInfo{
				LogLevel: "INFO", Telemetry: bootstrapconfig.TelemetryInfo{Interval: "30s"},
			}}}
			manager := &reloadMetricsManager{}
			notifications := make(UpdatedStream, 2)
			cp := &Processor{ctx: context.Background(), lc: logger.NewMockClient(), configUpdated: notifications,
				dic: di.NewContainer(di.ServiceConstructorMap{
					container.MetricsManagerInterfaceName: func(di.Get) any {
						if tc.noManager {
							return nil
						}
						return manager
					},
				}),
				commonConfigClient: snapshotConfigProvider{load: func(target *snapshotReloadConfig) (any, error) {
					target.Writable = live.Writable
					target.Writable.Telemetry.Interval = tc.interval
					target.Writable.StoreAndForward.Enabled = true
					return target, nil
				}},
			}
			cp.applyWritableUpdates(live)
			require.Equal(t, tc.wantResets, manager.intervals)
			require.Len(t, notifications, 1)
		})
	}
}
