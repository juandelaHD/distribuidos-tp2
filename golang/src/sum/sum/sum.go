package sum

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

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

type Sum struct {
	id             int
	sumAmount      int
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	ringIn         middleware.Middleware
	ringOut        middleware.Middleware

	mu             sync.Mutex
	fruitItemMaps  map[inner.ClientID]map[string]fruititem.FruitItem
	rowCounts      map[inner.ClientID]uint32
	expectedTotals map[inner.ClientID]uint32
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	ringInName := fmt.Sprintf("%s_ring_%d", config.SumPrefix, config.Id)
	ringIn, err := middleware.CreateQueueMiddleware(ringInName, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	ringOutName := fmt.Sprintf("%s_ring_%d", config.SumPrefix, (config.Id+1)%config.SumAmount)
	ringOut, err := middleware.CreateQueueMiddleware(ringOutName, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		ringIn.Close()
		return nil, err
	}

	return &Sum{
		id:             config.Id,
		sumAmount:      config.SumAmount,
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		ringIn:         ringIn,
		ringOut:        ringOut,
		fruitItemMaps:  map[inner.ClientID]map[string]fruititem.FruitItem{},
		rowCounts:      map[inner.ClientID]uint32{},
		expectedTotals: map[inner.ClientID]uint32{},
	}, nil
}

func (sum *Sum) Run() {
	go func() {
		if err := sum.ringIn.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleRingMessage(msg, ack, nack)
		}); err != nil {
			slog.Error("Ring consumer stopped", "err", err)
		}
	}()

	if err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleInputMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("Input queue consumer stopped", "err", err)
	}
}

func (sum *Sum) handleInputMessage(msg middleware.Message, ack func(), nack func()) {
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
	sum.mu.Lock()
	defer sum.mu.Unlock()

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
	sum.mu.Lock()
	sum.expectedTotals[clientID] = total
	myCount := sum.rowCounts[clientID]
	sum.mu.Unlock()

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

func (sum *Sum) handleRingMessage(msg middleware.Message, ack func(), nack func()) {
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

	sum.mu.Lock()
	ring.Count += sum.rowCounts[clientID]
	sum.mu.Unlock()

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
	sum.mu.Lock()
	clientMap := sum.fruitItemMaps[clientID]
	delete(sum.fruitItemMaps, clientID)
	delete(sum.rowCounts, clientID)
	delete(sum.expectedTotals, clientID)
	sum.mu.Unlock()

	slog.Info("Flushing accumulated data to aggregation", "client", clientID, "fruits", len(clientMap))

	for key := range clientMap {
		fruitRecord := []fruititem.FruitItem{clientMap[key]}
		message, err := inner.SerializeMessage(clientID, fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return
		}
	}

	eofMessage, err := inner.SerializeEOFMessage(clientID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return
	}
	if err := sum.outputExchange.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
	}
}

func (sum *Sum) cleanClientState(clientID inner.ClientID) {
	sum.mu.Lock()
	defer sum.mu.Unlock()
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
