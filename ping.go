package chconn

import (
	"context"
)

type pong struct{}

// Check that connection to the server is alive.
func (ch *conn) Ping(ctx context.Context) error {
	if ctx != context.Background() {
		select {
		case <-ctx.Done():
			return newContextAlreadyDoneError(ctx)
		default:
		}
		ch.contextWatcher.Watch(ctx)
		defer ch.contextWatcher.Unwatch()
	}
	ch.writer.Uvarint(clientPing)
	var hasError bool
	defer func() {
		if hasError {
			ch.Close()
		}
	}()
	if err := ch.flushWriteData(); err != nil {
		hasError = true
		return &writeError{"ping: write packet type", preferContextOverNetTimeoutError(ctx, err)}
	}

	res, err := ch.receiveAndProcessData(emptyOnProgress)
	if err != nil {
		hasError = true
		return preferContextOverNetTimeoutError(ctx, err)
	}
	if _, ok := res.(*pong); !ok {
		hasError = true
		return &unexpectedPacket{expected: "serverPong", actual: res}
	}

	return nil
}
