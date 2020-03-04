package chconn

import (
	"context"
)

func (ch *Conn) Ping(ctx context.Context) error {

	if err := ch.writer.Uvarint(clientPing); err != nil {
		return err
	}
	if _, err := ch.writer.WriteTo(ch.conn); err != nil {
		return err
	}

	packet, err := ch.reader.Uvarint()
	if err != nil {
		return err
	}

	// Could receive late packets with progress. TODO: Maybe possible to fix.
	for packet == serverProgress {
		progress := NewProgress()
		if err = progress.Read(ch); err != nil {
			return err
		}

		packet, err = ch.reader.Uvarint()
		if err != nil {
			return err
		}
	}

	if packet != serverPong {
		return &unexpectedPacket{expected: serverHello, actual: packet}
	}

	return nil
}
