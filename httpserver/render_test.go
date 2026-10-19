package httpserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderError(t *testing.T) {
	t.Run("preserves client error", func(t *testing.T) {
		response := httptest.NewRecorder()
		RenderError(response, http.StatusBadRequest, errors.New("invalid email"))
		var body map[string]string
		require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
		assert.Equal(t, "invalid email", body["message"])
	})

	t.Run("hides internal detail", func(t *testing.T) {
		response := httptest.NewRecorder()
		RenderError(response, http.StatusInternalServerError, errors.New("password=secret"))
		assert.NotContains(t, response.Body.String(), "secret")
	})
}
