package pg_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gearno.de/kit/pg"
)

func TestListenRejectsEmptyChannel(t *testing.T) {
	client, err := pg.NewClient(
		pg.WithRegisterer(prometheus.NewRegistry()),
	)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	err = client.Listen(context.Background(), "", func(string) {})
	require.Error(t, err)

	err = client.Notify(context.Background(), "", "payload")
	require.Error(t, err)
}

func TestListenReceivesNotification(t *testing.T) {
	client := newTestClient(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	channel := fmt.Sprintf("kit_listen_%d", time.Now().UnixNano())
	received := make(chan string, 1)
	done := make(chan error, 1)

	go func() {
		done <- client.Listen(
			ctx,
			channel,
			func(payload string) {
				received <- payload
				cancel()
			},
		)
	}()

	require.Eventually(
		t,
		func() bool {
			err := client.Notify(ctx, channel, "hello")
			if err != nil {
				return false
			}

			select {
			case payload := <-received:
				assert.Equal(t, "hello", payload)
				return true
			case <-time.After(20 * time.Millisecond):
				return false
			}
		},
		4*time.Second,
		20*time.Millisecond,
	)

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("listener did not return after cancellation")
	}
}
