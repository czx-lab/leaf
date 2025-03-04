package protobuf

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/czx-lab/leaf/chanrpc"
	"github.com/czx-lab/leaf/log"
	"github.com/czx-lab/leaf/network"
	"google.golang.org/protobuf/proto"
)

// -------------------------
// | id | protobuf message |
// -------------------------
type ProcessorV1 struct {
	littleEndian bool
	msgInfo      map[reflect.Type]*MsgInfoV1
	msgID        map[reflect.Type]uint16
	typeID       map[uint16]reflect.Type
}

type MsgInfoV1 struct {
	msgType       reflect.Type
	msgID         uint16
	msgRouter     *chanrpc.Server
	msgHandler    MsgHandler
	msgRawHandler MsgHandler
}

func NewMsgIdProcessor() *ProcessorV1 {
	return &ProcessorV1{
		msgInfo: make(map[reflect.Type]*MsgInfoV1),
		msgID:   make(map[reflect.Type]uint16),
		typeID:  make(map[uint16]reflect.Type),
	}
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *ProcessorV1) SetByteOrder(littleEndian bool) {
	p.littleEndian = littleEndian
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *ProcessorV1) SetRouter(msg proto.Message, msgRouter *chanrpc.Server) {
	msgType := reflect.TypeOf(msg)
	if _, ok := p.msgID[msgType]; !ok {
		log.Fatal("message %s not registered", msgType)
	}

	p.msgInfo[msgType].msgRouter = msgRouter
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *ProcessorV1) SetHandler(msg proto.Message, msgHandler MsgHandler) {
	msgType := reflect.TypeOf(msg)
	if _, ok := p.msgID[msgType]; !ok {
		log.Fatal("message %s not registered", msgType)
	}

	p.msgInfo[msgType].msgHandler = msgHandler
}

// It's dangerous to call the method on routing or marshaling (unmarshaling)
func (p *ProcessorV1) SetRawHandler(id uint16, msgRawHandler MsgHandler) {
	msgType, ok := p.typeID[id]
	if !ok {
		log.Fatal("message id %v not registered", id)
	}

	p.msgInfo[msgType].msgRawHandler = msgRawHandler
}

// goroutine safe
func (p *ProcessorV1) Range(f func(id uint16, t reflect.Type)) {
	for _, i := range p.msgInfo {
		f(uint16(i.msgID), i.msgType)
	}
}

// Marshal implements network.Processor.
func (p *ProcessorV1) Marshal(msg any) ([][]byte, error) {
	msgType := reflect.TypeOf(msg)
	i, ok := p.msgInfo[msgType]
	if !ok {
		return nil, fmt.Errorf("protobuf: message %v not registered", msgType)
	}

	id := make([]byte, 2)
	if p.littleEndian {
		binary.LittleEndian.PutUint16(id, i.msgID)
	} else {
		binary.BigEndian.PutUint16(id, i.msgID)
	}

	// data
	data, err := proto.Marshal(msg.(proto.Message))
	return [][]byte{id, data}, err
}

// Route implements network.Processor.
func (p *ProcessorV1) Route(msg, userData any) error {
	// raw
	if msgRaw, ok := msg.(MsgRaw); ok {
		msgType, ok := p.typeID[msgRaw.msgID]
		if !ok {
			return fmt.Errorf("message id %v not registered", msgRaw.msgID)
		}
		i := p.msgInfo[msgType]
		if i.msgRawHandler != nil {
			i.msgRawHandler([]any{msgRaw.msgID, msgRaw.msgRawData, userData})
		}
		return nil
	}

	// protobuf
	msgType := reflect.TypeOf(msg)
	if _, ok := p.msgID[msgType]; !ok {
		return fmt.Errorf("message %s not registered", msgType)
	}
	i := p.msgInfo[msgType]
	if i.msgHandler != nil {
		i.msgHandler([]any{msg, userData})
	}
	if i.msgRouter != nil {
		i.msgRouter.Go(msgType, msg, userData)
	}
	return nil
}

// Unmarshal implements network.Processor.
func (p *ProcessorV1) Unmarshal(data []byte) (any, error) {
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

	msgType, ok := p.typeID[id]
	if !ok {
		return nil, fmt.Errorf("protobuf: message ID %d not registered", id)
	}

	// msg
	i := p.msgInfo[msgType]
	if i.msgRawHandler != nil {
		return MsgRaw{id, data[2:]}, nil
	} else {
		msg := reflect.New(i.msgType.Elem()).Interface()
		return msg, proto.Unmarshal(data[2:], msg.(proto.Message))
	}
}

func (p *ProcessorV1) Register(msgID uint16, msg proto.Message) {
	msgType := reflect.TypeOf(msg)
	if msgType == nil || msgType.Kind() != reflect.Ptr {
		log.Fatal("protobuf: message must be a pointer")
	}
	if _, ok := p.msgInfo[msgType]; ok {
		log.Fatal("protobuf: message %v is already registered", msgType)
	}
	if len(p.msgInfo) >= math.MaxUint16 {
		log.Fatal("too many protobuf messages (max = %v)", math.MaxUint16)
	}

	p.msgInfo[msgType] = &MsgInfoV1{
		msgType: msgType, msgID: msgID,
	}
	p.msgID[msgType] = msgID
	p.typeID[msgID] = msgType
}

var _ network.Processor = (*ProcessorV1)(nil)
