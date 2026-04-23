package sum

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

func (sum *Sum) handleInputMessage(msg middleware.Message, ack func()) {
	defer ack()

	clientID, fruitRecords, isEof, total, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing input message", "err", err)
		return
	}

	if isEof {
		sum.startCollectRound(clientID, total)
		return
	}

	sum.accumulateData(clientID, fruitRecords)
}

func (sum *Sum) accumulateData(clientID inner.ClientID, fruitRecords []fruititem.FruitItem) {
	clientMap, clientExists := sum.fruitItemMaps[clientID]
	if !clientExists {
		clientMap = map[string]fruititem.FruitItem{}
		sum.fruitItemMaps[clientID] = clientMap
	}

	for _, fruitRecord := range fruitRecords {
		if existing, fruitExists := clientMap[fruitRecord.Fruit]; fruitExists {
			clientMap[fruitRecord.Fruit] = existing.Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
		sum.rowCounts[clientID]++
	}
}

func (sum *Sum) startCollectRound(clientID inner.ClientID, total uint32) {
	sum.expectedTotals[clientID] = total
	myCount := sum.rowCounts[clientID]

	slog.Debug("Received EOF, starting collect round", "client", clientID, "total", total, "my_count", myCount)

	collect := ringMessage{
		Type:      ringCollect,
		ClientID:  uint32(clientID),
		Initiator: sum.id,
		Total:     total,
		Count:     myCount,
	}
	if err := sum.sendRing(collect); err != nil {
		slog.Error("While sending collect message", "client", clientID, "err", err)
	}
}
