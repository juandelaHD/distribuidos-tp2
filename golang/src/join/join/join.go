package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	aggregationAmount int
	topSize           int
	partials  map[inner.ClientID][]fruititem.FruitItem
	eofCounts map[inner.ClientID]int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		aggregationAmount: config.AggregationAmount,
		topSize:           config.TopSize,
		partials:          map[inner.ClientID][]fruititem.FruitItem{},
		eofCounts:         map[inner.ClientID]int{},
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientID, fruitRecords, isEof, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		join.handleEOF(clientID)
		return
	}

	join.partials[clientID] = append(join.partials[clientID], fruitRecords...)
}

func (join *Join) handleEOF(clientID inner.ClientID) {
	join.eofCounts[clientID]++
	slog.Debug("Received partial top EOF", "client", clientID, "count", join.eofCounts[clientID], "expected", join.aggregationAmount)

	if join.eofCounts[clientID] < join.aggregationAmount {
		return
	}

	finalTop := join.buildFinalTop(clientID)
	delete(join.partials, clientID)
	delete(join.eofCounts, clientID)

	slog.Info("All partial tops received, emitting final top", "client", clientID, "size", len(finalTop))

	message, err := inner.SerializeMessage(clientID, finalTop)
	if err != nil {
		slog.Error("While serializing final top", "client", clientID, "err", err)
		return
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Error("While sending final top", "client", clientID, "err", err)
		return
	}

	eofMessage, err := inner.SerializeEOFMessage(clientID)
	if err != nil {
		slog.Error("While serializing final EOF", "client", clientID, "err", err)
		return
	}
	if err := join.outputQueue.Send(*eofMessage); err != nil {
		slog.Error("While sending final EOF", "client", clientID, "err", err)
	}
}

func (join *Join) buildFinalTop(clientID inner.ClientID) []fruititem.FruitItem {
	fruits := join.partials[clientID]
	sort.SliceStable(fruits, func(i, j int) bool {
		return fruits[j].Less(fruits[i])
	})
	finalSize := min(join.topSize, len(fruits))
	return fruits[:finalSize]
}
