package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/container"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/bootstrap/interfaces"
	bootstrapConfig "github.com/edgexfoundry/go-mod-bootstrap/v4/config"
	"github.com/edgexfoundry/go-mod-bootstrap/v4/di"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	commonDTO "github.com/edgexfoundry/go-mod-core-contracts/v4/dtos/common"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type apiSnapshotConfig struct {
	TestConfig
	calls int
	fail  bool
}

func (c *apiSnapshotConfig) GetConfigurationSnapshot() (interfaces.Configuration, error) {
	c.calls++
	if c.fail {
		return nil, fmt.Errorf("snapshot unavailable")
	}
	return &apiSnapshotConfig{TestConfig: TestConfig{Service: bootstrapConfig.ServiceInfo{Host: "snapshot-host"}}}, nil
}
func TestConfigRequestUsesSnapshot(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed=%t", fail), func(t *testing.T) {
			live := &apiSnapshotConfig{TestConfig: TestConfig{Service: bootstrapConfig.ServiceInfo{Host: "live-host"}}, fail: fail}
			dic := mockDic()
			dic.Update(di.ServiceConstructorMap{container.ConfigurationInterfaceName: func(di.Get) any { return live }})
			target := NewCommonController(dic, echo.New(), "test", serviceVersion)
			recorder := httptest.NewRecorder()
			require.NoError(t, target.Config(echo.New().NewContext(httptest.NewRequest(http.MethodGet, common.ApiConfigRoute, nil), recorder)))
			require.Equal(t, 1, live.calls)
			if fail {
				require.Equal(t, http.StatusInternalServerError, recorder.Code)
				return
			}
			require.Equal(t, http.StatusOK, recorder.Code)
			var response commonDTO.ConfigResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			encoded, err := json.Marshal(response.Config)
			require.NoError(t, err)
			var actual struct{ TestConfig TestConfig }
			require.NoError(t, json.Unmarshal(encoded, &actual))
			require.Equal(t, "snapshot-host", actual.TestConfig.Service.Host)
		})
	}
}
