package sum

import (
	"encoding/json"
	"hash/fnv"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type ringMessageType string

const (
	ringCollect ringMessageType = "collect"
	ringSend    ringMessageType = "send"
)

type ringMessage struct {
	Type      ringMessageType `json:"type"`
	ClientID  uint32          `json:"client"`
	Initiator int             `json:"initiator"`
	Total     uint32          `json:"total,omitempty"`
	Count     uint32          `json:"count,omitempty"`
}

// shardIndex mapea una fruta a un índice de aggregator.
// Todos los Sums corren el mismo binario, así que siempre dan el mismo resultado
// para el mismo input. Eso garantiza que cada fruta viva en un único shard.
func shardIndex(fruit string, shards int) int {
	h := fnv.New32a()
	h.Write([]byte(fruit))
	return int(h.Sum32() % uint32(shards))
}

func (sum *Sum) handleRingMessage(msg middleware.Message, ack func()) {
	defer ack()

	var ring ringMessage
	if err := json.Unmarshal([]byte(msg.Body), &ring); err != nil {
		slog.Error("While deserializing ring message", "err", err)
		return
	}

	switch ring.Type {
	case ringCollect:
		sum.handleCollect(ring)
	case ringSend:
		sum.handleSend(ring)
	default:
		slog.Error("Unknown ring message type", "type", ring.Type)
	}
}

func (sum *Sum) handleCollect(ring ringMessage) {
	clientID := inner.ClientID(ring.ClientID)

	if ring.Initiator == sum.id {
		if ring.Count == ring.Total {
			slog.Debug("Collect round complete, counts match — starting send round", "client", clientID, "total", ring.Total)
			sum.flushClientData(clientID)
			sendMsg := ringMessage{Type: ringSend, ClientID: ring.ClientID, Initiator: sum.id}
			if err := sum.sendRing(sendMsg); err != nil {
				slog.Error("While sending send message", "client", clientID, "err", err)
			}
			return
		}
		slog.Debug("Collect round mismatch, re-enqueueing EOF", "client", clientID, "count", ring.Count, "total", ring.Total)
		eofMsg, err := inner.SerializeEOFMessageWithTotal(clientID, ring.Total)
		if err != nil {
			slog.Error("While serializing re-enqueue EOF", "client", clientID, "err", err)
			return
		}
		if err := sum.inputQueue.Send(*eofMsg); err != nil {
			slog.Error("While re-enqueueing EOF", "client", clientID, "err", err)
		}
		return
	}

	ring.Count += sum.rowCounts[clientID]

	if err := sum.sendRing(ring); err != nil {
		slog.Error("While forwarding collect message", "client", clientID, "err", err)
	}
}

func (sum *Sum) handleSend(ring ringMessage) {
	clientID := inner.ClientID(ring.ClientID)

	if ring.Initiator == sum.id {
		slog.Debug("Send round complete, terminating", "client", clientID)
		sum.cleanClientState(clientID)
		return
	}

	sum.flushClientData(clientID)

	if err := sum.sendRing(ring); err != nil {
		slog.Error("While forwarding send message", "client", clientID, "err", err)
	}
}

func (sum *Sum) flushClientData(clientID inner.ClientID) {
	clientMap := sum.fruitItemMaps[clientID]
	delete(sum.fruitItemMaps, clientID)
	delete(sum.rowCounts, clientID)
	delete(sum.expectedTotals, clientID)

	slog.Info("Flushing accumulated data to aggregation", "client", clientID, "fruits", len(clientMap))

	shardCount := len(sum.outputQueues)
	for key := range clientMap {
		fruitRecord := []fruititem.FruitItem{clientMap[key]}
		message, err := inner.SerializeMessage(clientID, fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return
		}
		shard := shardIndex(clientMap[key].Fruit, shardCount)
		if err := sum.outputQueues[shard].Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return
		}
	}

	eofMessage, err := inner.SerializeEOFMessage(clientID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return
	}
	for _, q := range sum.outputQueues {
		if err := q.Send(*eofMessage); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return
		}
	}
}

func (sum *Sum) cleanClientState(clientID inner.ClientID) {
	delete(sum.fruitItemMaps, clientID)
	delete(sum.rowCounts, clientID)
	delete(sum.expectedTotals, clientID)
}

func (sum *Sum) sendRing(ring ringMessage) error {
	body, err := json.Marshal(ring)
	if err != nil {
		return err
	}
	return sum.ringOut.Send(middleware.Message{Body: string(body)})
}
