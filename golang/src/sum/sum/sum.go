package sum

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

// shardIndex mapea una fruta a un índice de aggregator usando FNV-1a 32-bit.
// Todos los Sums corren el mismo binario, así que siempre dan el mismo resultado
// para el mismo input. Eso garantiza que cada fruta viva en un único shard.
func shardIndex(fruit string, shards int) int {
	h := fnv.New32a()
	h.Write([]byte(fruit))
	return int(h.Sum32() % uint32(shards))
}

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

const consumerCount = 2

type ringMessage struct {
	Type      ringMessageType `json:"type"`
	ClientID  uint32          `json:"client"`
	Initiator int             `json:"initiator"`
	Total     uint32          `json:"total,omitempty"`
	Count     uint32          `json:"count,omitempty"`
}

type Sum struct {
	id           int
	sumAmount    int
	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware
	ringIn       middleware.Middleware
	ringOut      middleware.Middleware

	mu             sync.Mutex
	fruitItemMaps  map[inner.ClientID]map[string]fruititem.FruitItem
	rowCounts      map[inner.ClientID]uint32
	expectedTotals map[inner.ClientID]uint32

	running      atomic.Bool
	consumers    sync.WaitGroup
	shutdownOnce sync.Once
	done         chan struct{}
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueues := make([]middleware.Middleware, 0, config.AggregationAmount)
	closeOutputs := func() {
		for _, q := range outputQueues {
			q.Close()
		}
	}
	for i := range config.AggregationAmount {
		queueName := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
		q, err := middleware.CreateQueueMiddleware(queueName, connSettings)
		if err != nil {
			inputQueue.Close()
			closeOutputs()
			return nil, err
		}
		outputQueues = append(outputQueues, q)
	}

	ringInName := fmt.Sprintf("%s_ring_%d", config.SumPrefix, config.Id)
	ringIn, err := middleware.CreateQueueMiddleware(ringInName, connSettings)
	if err != nil {
		inputQueue.Close()
		closeOutputs()
		return nil, err
	}

	ringOutName := fmt.Sprintf("%s_ring_%d", config.SumPrefix, (config.Id+1)%config.SumAmount)
	ringOut, err := middleware.CreateQueueMiddleware(ringOutName, connSettings)
	if err != nil {
		inputQueue.Close()
		closeOutputs()
		ringIn.Close()
		return nil, err
	}

	sum := &Sum{
		id:             config.Id,
		sumAmount:      config.SumAmount,
		inputQueue:     inputQueue,
		outputQueues:   outputQueues,
		ringIn:         ringIn,
		ringOut:        ringOut,
		fruitItemMaps:  map[inner.ClientID]map[string]fruititem.FruitItem{},
		rowCounts:      map[inner.ClientID]uint32{},
		expectedTotals: map[inner.ClientID]uint32{},
		done:           make(chan struct{}),
	}
	sum.running.Store(true)
	return sum, nil
}

func (sum *Sum) Run() error {
	go sum.handleSignals()

	sum.consumers.Add(consumerCount)
	go sum.runConsumer(sum.ringIn, "ring", func(msg middleware.Message, ack, nack func()) {
		sum.handleRingMessage(msg, ack, nack)
	})
	go sum.runConsumer(sum.inputQueue, "input", func(msg middleware.Message, ack, nack func()) {
		sum.handleInputMessage(msg, ack, nack)
	})

	sum.consumers.Wait()
	return sum.closeAll()
}

func (sum *Sum) runConsumer(queue middleware.Middleware, name string, cb func(middleware.Message, func(), func())) {
	defer sum.consumers.Done()
	err := queue.StartConsuming(cb)
	if err != nil && sum.running.Load() {
		slog.Error("Consumer stopped unexpectedly", "name", name, "err", err)
		sum.shutdown()
	}
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	select {
	case <-signals:
		sum.shutdown()
	case <-sum.done:
		// Acá se manejaría el caso de una falla inesperada de consumer que dispare el shutdown desde otro lado que no sea la señal.
		// El TP asume que ningún nodo se cae, así que en el camino normal el shutdown siempre lo dispara SIGTERM.	
	}
}

func (sum *Sum) shutdown() {
	sum.shutdownOnce.Do(func() {
		slog.Info("SIGTERM received, shutting down")
		sum.running.Store(false)
		if err := sum.inputQueue.StopConsuming(); err != nil {
			slog.Debug("While stopping input consumer", "err", err)
		}
		if err := sum.ringIn.StopConsuming(); err != nil {
			slog.Debug("While stopping ring consumer", "err", err)
		}
		close(sum.done)
	})
}

func (sum *Sum) closeAll() error {
	if err := sum.inputQueue.Close(); err != nil {
		slog.Debug("While closing input queue", "err", err)
	}
	if err := sum.ringIn.Close(); err != nil {
		slog.Debug("While closing ring in", "err", err)
	}
	if err := sum.ringOut.Close(); err != nil {
		slog.Debug("While closing ring out", "err", err)
	}
	for i, q := range sum.outputQueues {
		if err := q.Close(); err != nil {
			slog.Debug("While closing output shard", "index", i, "err", err)
		}
	}
	return nil
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
