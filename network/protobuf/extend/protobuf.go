package extend

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"reflect"

	"github.com/czx-lab/leaf/chanrpc"
	"github.com/czx-lab/leaf/network"
	"google.golang.org/protobuf/proto"
)

type MsgHandler func([]any)

type MsgInfo struct {
	msgType       reflect.Type
	msgID         uint16
	msgRouter     *chanrpc.Server
	msgHandler    MsgHandler
	msgRawHandler MsgHandler
}

type MsgRaw struct {
	msgID      uint16
	msgRawData []byte
}

// -------------------------
// | id | protobuf message |
// -------------------------
type Processor struct {
	littleEndian bool
	msgInfo      map[uint16]*MsgInfo
	msgID        map[reflect.Type]uint16
}

// Marshal implements network.Processor.
func (p *Processor) Marshal(msg any) ([][]byte, error) {
	msgType := reflect.TypeOf(msg)
	msgId, ok := p.msgID[msgType]
	if !ok {
		return nil, fmt.Errorf("protobuf: message %v not registered", msgType)
	}

	id := make([]byte, 2)
	if p.littleEndian {
		binary.LittleEndian.PutUint16(id, msgId)
	} else {
		binary.BigEndian.PutUint16(id, msgId)
	}

	// data
	data, err := proto.Marshal(msg.(proto.Message))
	return [][]byte{id, data}, err
}

// Route implements network.Processor.
func (p *Processor) Route(msg, userData any) error {
	// raw
	if msgRaw, ok := msg.(MsgRaw); ok {
		info, ok := p.msgInfo[msgRaw.msgID]
		if !ok {
			return fmt.Errorf("message id %v not registered", msgRaw.msgID)
		}
		if info.msgRawHandler != nil {
			info.msgRawHandler([]any{msgRaw.msgID, msgRaw.msgRawData, userData})
		}
		return nil
	}

	// protobuf
	msgType := reflect.TypeOf(msg)
	id, ok := p.msgID[msgType]
	if !ok {
		return fmt.Errorf("message %s not registered", msgType)
	}

	info := p.msgInfo[id]
	if info.msgHandler != nil {
		info.msgHandler([]any{msg, userData})
	}
	if info.msgRouter != nil {
		info.msgRouter.Go(msgType, msg, userData)
	}
	return nil
}

// Unmarshal implements network.Processor.
func (p *Processor) Unmarshal(data []byte) (any, error) {
	if len(data) < 2 {
		return nil, errors.New("protobuf data too short")
	}

	// id
	var id uint16
	if p.littleEndian {
		id = binary.LittleEndian.Uint16(data)
	} else {
		id = binary.BigEndian.Uint16(data)
	}

	info, ok := p.msgInfo[id]
	if !ok {
		return nil, fmt.Errorf("protobuf: message ID %d not registered", id)
	}
	if info.msgRawHandler != nil {
		return MsgRaw{id, data[2:]}, nil
	}

	msg := reflect.New(info.msgType.Elem()).Interface()
	return msg, proto.Unmarshal(data[2:], msg.(proto.Message))
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) Register(msgID uint16, msg proto.Message) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("protobuf: message must be a pointer")
	}

	id, ok := p.msgID[msgType]
	if ok {
		log.Fatal("protobuf: message %v is already registered", msgType)
	}
	if len(p.msgInfo) >= math.MaxUint16 {
		log.Fatal("too many protobuf messages (max = %v)", math.MaxUint16)
	}

	p.msgInfo[id] = &MsgInfo{
		msgType: msgType,
		msgID:   msgID,
	}
	p.msgID[msgType] = msgID
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetByteOrder(littleEndian bool) {
	p.littleEndian = littleEndian
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetRouter(msg proto.Message, msgRouter *chanrpc.Server) {
	msgType := reflect.TypeOf(msg)
	id, ok := p.msgID[msgType]
	if !ok {
		log.Fatal("message %s not registered", msgType)
	}

	p.msgInfo[id].msgRouter = msgRouter
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetHandler(msg proto.Message, msgHandler MsgHandler) {
	msgType := reflect.TypeOf(msg)
	id, ok := p.msgID[msgType]
	if !ok {
		log.Fatal("message %s not registered", msgType)
	}

	p.msgInfo[id].msgHandler = msgHandler
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *Processor) SetRawHandler(id uint16, msgRawHandler MsgHandler) {
	info, ok := p.msgInfo[id]
	if !ok {
		log.Fatal("message id %v not registered", id)
	}

	info.msgRawHandler = msgRawHandler
}

// goroutine safe
func (p *Processor) Range(f func(id uint16, t reflect.Type)) {
	for _, i := range p.msgInfo {
		f(uint16(i.msgID), i.msgType)
	}
}

var _ network.Processor = (*Processor)(nil)
