package pg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryAttributesRedactSQLByDefault(t *testing.T) {
	attrs := (&tracer{}).queryAttributes("SELECT 'secret'")
	assert.Len(t, attrs, 1)
	assert.Equal(t, "SELECT", attrs[0].Value.AsString())
}

func TestQueryAttributesCanIncludeSQL(t *testing.T) {
	attrs := (&tracer{recordQueryText: true}).queryAttributes("SELECT 'secret'")
	assert.Len(t, attrs, 2)
	assert.Equal(t, "SELECT 'secret'", attrs[1].Value.AsString())
}
