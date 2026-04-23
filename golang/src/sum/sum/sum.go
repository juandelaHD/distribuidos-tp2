package sum

import (
	"fmt"
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

const (
	consumerCount   = 2
	eventBufferSize = 256
)

type eventSource string

const (
	sourceInput eventSource = "input"
	sourceRing  eventSource = "ring"
)

type event struct {
	msg    middleware.Message
	ack    func()
	source eventSource
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

type Sum struct {
	id           int
	sumAmount    int
	inputQueue   middleware.Middleware
	outputQueues []middleware.Middleware
	ringIn       middleware.Middleware
	ringOut      middleware.Middleware

	fruitItemMaps  map[inner.ClientID]map[string]fruititem.FruitItem
	rowCounts      map[inner.ClientID]uint32
	expectedTotals map[inner.ClientID]uint32

	events        chan event
	eventLoopDone chan struct{}

	running      atomic.Bool
	consumers    sync.WaitGroup
	shutdownOnce sync.Once
	done chan struct{}
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
		events:         make(chan event, eventBufferSize),
		eventLoopDone:  make(chan struct{}),
		done:           make(chan struct{}),
	}
	sum.running.Store(true)
	return sum, nil
}

func (sum *Sum) Run() error {
	go sum.handleSignals()
	go sum.runEventLoop()

	sum.consumers.Add(consumerCount)
	go sum.runConsumer(sum.ringIn, sourceRing, func(msg middleware.Message, ack, _ func()) {
		sum.events <- event{msg: msg, ack: ack, source: sourceRing}
	})
	go sum.runConsumer(sum.inputQueue, sourceInput, func(msg middleware.Message, ack, _ func()) {
		sum.events <- event{msg: msg, ack: ack, source: sourceInput}
	})

	sum.consumers.Wait()
	close(sum.events)
	<-sum.eventLoopDone
	return sum.closeAll()
}

func (sum *Sum) runConsumer(queue middleware.Middleware, name eventSource, cb func(middleware.Message, func(), func())) {
	defer sum.consumers.Done()
	err := queue.StartConsuming(cb)
	if err != nil && sum.running.Load() {
		slog.Error("Consumer stopped unexpectedly", "name", name, "err", err)
		sum.shutdown()
	}
}

func (sum *Sum) runEventLoop() {
	defer close(sum.eventLoopDone)
	for ev := range sum.events {
		switch ev.source {
		case sourceInput:
			sum.handleInputMessage(ev.msg, ev.ack)
		case sourceRing:
			sum.handleRingMessage(ev.msg, ev.ack)
		}
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
