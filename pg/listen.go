// Copyright (c) 2024 Bryan Frimin <bryan@frimin.fr>.
//
// Permission to use, copy, modify, and/or distribute this software
// for any purpose with or without fee is hereby granted, provided
// that the above copyright notice and this permission notice appear
// in all copies.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
// WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE
// AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
// CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
// OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
// NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
// CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

package pg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Listen opens a dedicated connection and calls handler for each
// notification delivered on channel. It returns when ctx is canceled
// or the connection fails. The caller decides whether to reconnect.
//
// The connection is not taken from the pool. LISTEN holds it until
// the call returns, and the pool would otherwise recycle it.
func (c *Client) Listen(
	ctx context.Context,
	channel string,
	handler func(payload string),
) error {
	if channel == "" {
		return fmt.Errorf("cannot listen for PostgreSQL notifications: channel is empty")
	}

	if handler == nil {
		return fmt.Errorf("cannot listen for PostgreSQL notifications: handler is nil")
	}

	conn, err := pgx.ConnectConfig(ctx, c.connConfig)
	if err != nil {
		return fmt.Errorf("cannot connect PostgreSQL notification listener: %w", err)
	}

	defer func() { _ = conn.Close(context.Background()) }()

	listen := "LISTEN " + pgx.Identifier{channel}.Sanitize()
	if _, err := conn.Exec(ctx, listen); err != nil {
		return fmt.Errorf("cannot listen for PostgreSQL notifications: %w", err)
	}

	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("cannot wait for PostgreSQL notification: %w", err)
		}

		handler(notification.Payload)
	}
}

// Notify publishes payload on channel using a pooled connection.
//
// A notification that must commit together with a write has to run
// inside that transaction, with SELECT pg_notify($1, $2). This method
// publishes immediately on its own connection.
func (c *Client) Notify(
	ctx context.Context,
	channel string,
	payload string,
) error {
	if channel == "" {
		return fmt.Errorf("cannot notify PostgreSQL channel: channel is empty")
	}

	return c.WithConn(
		ctx,
		func(ctx context.Context, conn Querier) error {
			_, err := conn.Exec(ctx, "SELECT pg_notify($1, $2)", channel, payload)
			if err != nil {
				return fmt.Errorf("cannot notify PostgreSQL channel: %w", err)
			}

			return nil
		},
	)
}
