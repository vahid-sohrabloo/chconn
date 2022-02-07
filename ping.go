package chconn

import (
	"context"
)

type pong struct{}

// Check that connection to the server is alive.
func (ch *conn) Ping(ctx context.Context) error {
	ch.contextWatcher.Watch(ctx)
	defer ch.contextWatcher.Unwatch()
	ch.writer.Uvarint(clientPing)
	var hasError bool
	defer func() {
		if hasError {
			ch.Close()
		}
	}()
	if _, err := ch.writer.WriteTo(ch.writerTo); err != nil {
		hasError = true
		return &writeError{"ping: write packet type", err}
	}

	res, err := ch.receiveAndProccessData(emptyOnProgress)
	if err != nil {
		hasError = true
		return err
	}
	if _, ok := res.(*pong); !ok {
		hasError = true
		return &unexpectedPacket{expected: "serverPong", actual: res}
	}

	return nil
}
