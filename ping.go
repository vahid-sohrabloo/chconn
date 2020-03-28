package chconn

import (
	"context"
)

type pong struct{}

func (ch *conn) Ping(ctx context.Context) error {
	ch.contextWatcher.Watch(ctx)
	defer ch.contextWatcher.Unwatch()
	ch.writer.Uvarint(clientPing)
	if _, err := ch.writer.WriteTo(ch.writerto); err != nil {
		return &writeError{"ping: write packet type", err}
	}

	res, err := ch.reciveAndProccessData(emptyOnProgress)
	if err != nil {
		return err
	}
	if _, ok := res.(*pong); !ok {
		return &unexpectedPacket{expected: "serverPong", actual: res}
	}

	return nil
}
