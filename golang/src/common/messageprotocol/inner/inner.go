package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type ClientID uint32

type wireFruitItem struct {
	Fruit  string `json:"f"`
	Amount uint32 `json:"a"`
}

type wireMessage struct {
	ClientID ClientID        `json:"c"`
	Data     []wireFruitItem `json:"d"`
	Total    uint32          `json:"t,omitempty"`
}

func SerializeMessage(clientID ClientID, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	wireRecords := make([]wireFruitItem, 0, len(fruitRecords))
	for _, fruitRecord := range fruitRecords {
		wireRecords = append(wireRecords, wireFruitItem{
			Fruit:  fruitRecord.Fruit,
			Amount: fruitRecord.Amount,
		})
	}

	body, err := json.Marshal(wireMessage{ClientID: clientID, Data: wireRecords})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func SerializeEOFMessage(clientID ClientID) (*middleware.Message, error) {
	return SerializeMessage(clientID, nil)
}

func SerializeEOFMessageWithTotal(clientID ClientID, total uint32) (*middleware.Message, error) {
	body, err := json.Marshal(wireMessage{ClientID: clientID, Data: []wireFruitItem{}, Total: total})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeMessage(message *middleware.Message) (ClientID, []fruititem.FruitItem, bool, uint32, error) {
	var wire wireMessage
	if err := json.Unmarshal([]byte(message.Body), &wire); err != nil {
		return 0, nil, false, 0, err
	}

	fruitRecords := make([]fruititem.FruitItem, 0, len(wire.Data))
	for _, wireRecord := range wire.Data {
		fruitRecords = append(fruitRecords, fruititem.FruitItem{
			Fruit:  wireRecord.Fruit,
			Amount: wireRecord.Amount,
		})
	}

	return wire.ClientID, fruitRecords, len(fruitRecords) == 0, wire.Total, nil
}
