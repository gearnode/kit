package version

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizedVersion(t *testing.T) {
	assert.Equal(t, "devel", normalizedVersion(debug.Module{}))
	assert.Equal(t, "devel", normalizedVersion(debug.Module{Version: "(devel)"}))
	assert.Equal(t, "v1.2.3", normalizedVersion(debug.Module{Version: "v1.2.3"}))
}
