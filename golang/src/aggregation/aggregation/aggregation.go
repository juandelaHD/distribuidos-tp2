package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const consumerCount = 1

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputQueue    middleware.Middleware
	fruitItemMaps map[inner.ClientID]map[string]fruititem.FruitItem
	topSize       int
	sumAmount     int
	eofCounts     map[inner.ClientID]int

	running      atomic.Bool
	consumers    sync.WaitGroup
	shutdownOnce sync.Once
	done         chan struct{}
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputQueueName := fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)
	inputQueue, err := middleware.CreateQueueMiddleware(inputQueueName, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	aggregation := &Aggregation{
		outputQueue:   outputQueue,
		inputQueue:    inputQueue,
		fruitItemMaps: map[inner.ClientID]map[string]fruititem.FruitItem{},
		topSize:       config.TopSize,
		sumAmount:     config.SumAmount,
		eofCounts:     map[inner.ClientID]int{},
		done:          make(chan struct{}),
	}
	aggregation.running.Store(true)
	return aggregation, nil
}

func (aggregation *Aggregation) Run() error {
	go aggregation.handleSignals()

	aggregation.consumers.Add(consumerCount)
	go func() {
		defer aggregation.consumers.Done()
		err := aggregation.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			aggregation.handleMessage(msg, ack, nack)
		})
		if err != nil && aggregation.running.Load() {
			slog.Error("Input consumer stopped unexpectedly", "err", err)
			aggregation.shutdown()
		}
	}()

	aggregation.consumers.Wait()
	return aggregation.closeAll()
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	select {
	case <-signals:
		aggregation.shutdown()
	case <-aggregation.done:
		// Acá se manejaría el caso de una falla inesperada de consumer que dispare el shutdown desde otro lado que no sea la señal.
		// El TP asume que ningún nodo se cae, así que en el camino normal el shutdown siempre lo dispara SIGTERM.
	}
}

func (aggregation *Aggregation) shutdown() {
	aggregation.shutdownOnce.Do(func() {
		slog.Info("SIGTERM received, shutting down")
		aggregation.running.Store(false)
		if err := aggregation.inputQueue.StopConsuming(); err != nil {
			slog.Debug("While stopping input consumer", "err", err)
		}
		close(aggregation.done)
	})
}

func (aggregation *Aggregation) closeAll() error {
	if err := aggregation.inputQueue.Close(); err != nil {
		slog.Debug("While closing input queue", "err", err)
	}
	if err := aggregation.outputQueue.Close(); err != nil {
		slog.Debug("While closing output queue", "err", err)
	}
	return nil
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := aggregation.handleEndOfRecordsMessage(clientID); err != nil {
			slog.Error("While handling end of record message", "client", clientID, "err", err)
		}
		return
	}

	aggregation.handleDataMessage(clientID, fruitRecords)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID inner.ClientID) error {
	aggregation.eofCounts[clientID]++
	slog.Debug("Received End Of Records message", "client", clientID, "count", aggregation.eofCounts[clientID], "expected", aggregation.sumAmount)

	if aggregation.eofCounts[clientID] < aggregation.sumAmount {
		return nil
	}
	delete(aggregation.eofCounts, clientID)

	slog.Info("All partials received, sending top to join", "client", clientID, "top_size", aggregation.topSize)

	fruitTopRecords := aggregation.buildFruitTop(clientID)
	message, err := inner.SerializeMessage(clientID, fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage, err := inner.SerializeEOFMessage(clientID)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	delete(aggregation.fruitItemMaps, clientID)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID inner.ClientID, fruitRecords []fruititem.FruitItem) {
	clientMap, clientExists := aggregation.fruitItemMaps[clientID]
	if !clientExists {
		clientMap = map[string]fruititem.FruitItem{}
		aggregation.fruitItemMaps[clientID] = clientMap
	}

	for _, fruitRecord := range fruitRecords {
		if existing, fruitExists := clientMap[fruitRecord.Fruit]; fruitExists {
			clientMap[fruitRecord.Fruit] = existing.Sum(fruitRecord)
		} else {
			clientMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientID inner.ClientID) []fruititem.FruitItem {
	clientMap := aggregation.fruitItemMaps[clientID]
	fruitItems := make([]fruititem.FruitItem, 0, len(clientMap))
	for _, item := range clientMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
