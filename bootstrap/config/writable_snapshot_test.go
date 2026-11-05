package config

import (
	"fmt"
	"sync"
	"testing"

	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/edgexfoundry/go-mod-configuration/v4/configuration"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/stretchr/testify/require"
)

type snapshotReloadConfig struct {
	ConfigurationMockStruct
	mu          sync.RWMutex
	fullUpdates int
}

func (c *snapshotReloadConfig) UpdateFromRaw(raw any) bool {
	next, ok := raw.(*snapshotReloadConfig)
	if !ok {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ConfigurationMockStruct = next.ConfigurationMockStruct
	c.fullUpdates++
	return true
}
func (c *snapshotReloadConfig) UpdateWritableFromRaw(raw any) bool {
	next, ok := raw.(*WritableInfo)
	if !ok {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Writable = *next
	return true
}
func (c *snapshotReloadConfig) GetWritablePtr() any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	next := c.Writable
	return &next
}
func (c *snapshotReloadConfig) GetLogLevel() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Writable.LogLevel
}
func (c *snapshotReloadConfig) GetTelemetryInfo() *config.TelemetryInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	next := c.Writable.Telemetry
	return &next
}

type snapshotConfigProvider struct {
	configuration.Client
	load func(*snapshotReloadConfig) (any, error)
}

func (p snapshotConfigProvider) GetConfiguration(raw any) (any, error) {
	return p.load(raw.(*snapshotReloadConfig))
}

func TestWritableReloadPreservesStartupFieldsAndLayers(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed=%t", fail), func(t *testing.T) {
			live := &snapshotReloadConfig{ConfigurationMockStruct: ConfigurationMockStruct{
				Writable: WritableInfo{LogLevel: "INFO", Telemetry: config.TelemetryInfo{Interval: "30s"}, StoreAndForward: StoreAndForwardInfo{MaxRetryCount: 99}},
				Service:  config.ServiceInfo{Host: "startup-override"},
			}}
			cp := &Processor{lc: logger.NewMockClient(),
				commonConfigClient: snapshotConfigProvider{load: func(target *snapshotReloadConfig) (any, error) {
					require.NotSame(t, live, target)
					require.Empty(t, target.Writable, "provider decode target inherited live settings")
					target.Writable = WritableInfo{LogLevel: "INFO", Telemetry: config.TelemetryInfo{Interval: "30s"}, StoreAndForward: StoreAndForwardInfo{Enabled: true, MaxRetryCount: 1}}
					target.Service.Host = "provider-host"
					return target, nil
				}},
				privateConfigClient: snapshotConfigProvider{load: func(target *snapshotReloadConfig) (any, error) {
					require.Equal(t, 1, target.Writable.StoreAndForward.MaxRetryCount)
					if fail {
						return nil, fmt.Errorf("provider unavailable")
					}
					target.Writable.StoreAndForward.MaxRetryCount = 7
					return target, nil
				}},
			}
			cp.applyWritableUpdates(live)
			require.Zero(t, live.fullUpdates, "runtime reload replaced non-writable configuration")
			require.Equal(t, "startup-override", live.Service.Host)
			if fail {
				require.Equal(t, 99, live.Writable.StoreAndForward.MaxRetryCount)
			} else {
				require.Equal(t, 7, live.Writable.StoreAndForward.MaxRetryCount)
				require.True(t, live.Writable.StoreAndForward.Enabled)
			}
		})
	}
}

func TestWritableReloadDoesNotReadLiveFieldsWithoutAccessors(t *testing.T) {
	initial := WritableInfo{LogLevel: "INFO", Telemetry: config.TelemetryInfo{Interval: "30s"}}
	live := &snapshotReloadConfig{ConfigurationMockStruct: ConfigurationMockStruct{Writable: initial}}
	cp := &Processor{lc: logger.NewMockClient(), commonConfigClient: snapshotConfigProvider{load: func(target *snapshotReloadConfig) (any, error) {
		target.Writable = initial
		return target, nil
	}}}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 500; i++ {
			next := initial
			next.StoreAndForward.MaxRetryCount = i
			live.UpdateWritableFromRaw(&next)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 500; i++ {
			cp.applyWritableUpdates(live)
		}
	}()
	close(start)
	wg.Wait()
	require.Zero(t, live.fullUpdates)
}

type configurationSnapshotStub struct {
	ConfigurationMockStruct
	snapshotCalls int
}

func (c *configurationSnapshotStub) GetConfigurationSnapshot() (interfaces.Configuration, error) {
	c.snapshotCalls++
	return &configurationSnapshotStub{ConfigurationMockStruct: c.ConfigurationMockStruct}, nil
}
func TestStartupCopyUsesConfigurationSnapshot(t *testing.T) {
	live := &configurationSnapshotStub{ConfigurationMockStruct: ConfigurationMockStruct{Service: config.ServiceInfo{Host: "original"}}}
	snapshot, err := copyConfigurationStruct(live)
	require.NoError(t, err)
	require.Equal(t, 1, live.snapshotCalls)
	require.NotSame(t, live, snapshot)
	snapshot.GetBootstrap().Service.Host = "changed"
	require.Equal(t, "original", live.Service.Host)
}
