package utp_test

// The wire vectors of the TypeScript client (unit-io/unitdb-ts): frames as
// this package encodes them, with what each holds, written to -out. It runs
// in a unitdb checkout's server/utp by an overlay (scripts/vectors.mjs in
// the client), and is not part of the server.

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"testing"

	pbx "github.com/unit-io/unitdb/server/proto"
	"github.com/unit-io/unitdb/server/utp"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var out = flag.String("out", "", "where to write the vectors")

// vectorFile is the vectors.
type vectorFile struct {
	Comment string `json:"comment"`
	// Varints are the frame header's length prefix at its bounds.
	Varints []varint `json:"varints"`
	Frames  []frame  `json:"frames"`
	// Stream is several frames from the server back to back, to split at
	// every byte when checking a streaming decoder.
	Stream stream `json:"stream"`
	// Limits are the largest bodies a reader accepts.
	Limits limits `json:"limits"`
}

type varint struct {
	Value int    `json:"value"`
	Hex   string `json:"hex"`
}

type frame struct {
	Name string `json:"name"`
	// Direction is who sends it: "client" (the server decodes it) or
	// "server" (a client decodes it).
	Direction   string `json:"direction"`
	Hex         string `json:"hex"`
	MessageType int    `json:"messageType"`
	FlowControl int    `json:"flowControl"`
	// Body is the protobuf message the body holds ("unitdb.Publish"), and
	// Message its protobuf JSON; empty for a body of no bytes.
	Body    string          `json:"body,omitempty"`
	Message json.RawMessage `json:"message,omitempty"`
	// Connack is a CONNECT acknowledgement's ConnectAcknowledge, the
	// ControlMessage's Message decoded.
	Connack json.RawMessage `json:"connack,omitempty"`
}

type stream struct {
	Hex    string   `json:"hex"`
	Frames []string `json:"frames"` // their names, in order
}

type limits struct {
	MaxFrame int `json:"maxFrame"`
}

type encoder interface {
	ToBinary() (bytes.Buffer, error)
}

func TestTSVectors(t *testing.T) {
	if *out == "" {
		t.Skip("no -out")
	}
	f := vectorFile{
		Comment: "Written by the unitdb server's utp package (scripts/vectors.mjs). Each frame: a base-128 varint length, a unitdb.schema.FixedHeader, then the body.",
		Limits:  limits{MaxFrame: 4 << 20},
	}
	for _, v := range []int{0, 1, 127, 128, 300, 16383, 16384, 2097151, 2097152, 268435455} {
		f.Varints = append(f.Varints, varint{Value: v, Hex: hex.EncodeToString(utp.EncodeLength(v))})
	}

	big := bytes.Repeat([]byte("0123456789abcdef"), 4096) // 64 KiB
	connack := func(rc uint8, epoch, connID int32) []byte {
		b, err := (&utp.ConnectAcknowledge{ReturnCode: rc, Epoch: epoch, ConnID: connID}).ToBinary()
		if err != nil {
			t.Fatal(err)
		}
		return b.Bytes()
	}
	add := func(name, dir string, m encoder, body proto.Message) {
		t.Helper()
		b, err := m.ToBinary()
		if err != nil {
			t.Fatal(name, err)
		}
		fr := frame{Name: name, Direction: dir, Hex: hex.EncodeToString(b.Bytes())}
		var fh utp.FixedHeader
		r := bytes.NewReader(b.Bytes())
		if err := fh.FromBinary(r); err != nil {
			t.Fatal(name, err)
		}
		fr.MessageType, fr.FlowControl = int(fh.MessageType), int(fh.FlowControl)
		rest := b.Bytes()[len(b.Bytes())-r.Len():]
		if len(rest) != fh.MessageLength {
			t.Fatalf("%s: body %d bytes, header says %d", name, len(rest), fh.MessageLength)
		}
		if len(rest) > 0 {
			if err := proto.Unmarshal(rest, body); err != nil {
				t.Fatal(name, err)
			}
			fr.Body = string(body.ProtoReflect().Descriptor().FullName())
			fr.Message = protoJSON(t, body)
			if c, ok := body.(*pbx.ControlMessage); ok && fh.MessageType == utp.CONNECT && len(c.Message) > 0 {
				var ca pbx.ConnectAcknowledge
				if err := proto.Unmarshal(c.Message, &ca); err != nil {
					t.Fatal(name, err)
				}
				fr.Connack = protoJSON(t, &ca)
			}
		}
		f.Frames = append(f.Frames, fr)
	}
	ctrl := func(id uint16, mt utp.MessageType, fc utp.FlowControl, msg []byte) *utp.ControlMessage {
		return &utp.ControlMessage{MessageID: id, MessageType: mt, FlowControl: fc, Message: msg}
	}

	// From a client.
	add("connect", "client", &utp.Connect{Version: 2, ClientID: "AbCdEf-_0123456789", KeepAlive: 30, CleanSessFlag: true, SessKey: 42,
		Username: "alice@example.com", Password: []byte{0, 1, 2, 250, 255},
		BatchDuration: 500, BatchByteThreshold: 65536, BatchCountThreshold: 100}, &pbx.Connect{})
	add("connect minimal", "client", &utp.Connect{ClientID: "id"}, &pbx.Connect{})
	add("connect insecure", "client", &utp.Connect{InsecureFlag: true, ClientID: "id", SessKey: -7}, &pbx.Connect{})
	add("publish express", "client", &utp.Publish{MessageID: 1, Messages: []*utp.PublishMessage{{Topic: "key/groups.acme.chat", Payload: []byte("hello"), Ttl: "1h"}}}, &pbx.Publish{})
	add("publish reliable", "client", &utp.Publish{MessageID: 65535, DeliveryMode: 1, Messages: []*utp.PublishMessage{{Topic: "docs.acme.d.x", Payload: []byte(`{"v":1}`)}}}, &pbx.Publish{})
	add("publish batch of two", "client", &utp.Publish{MessageID: 300, DeliveryMode: 2, Messages: []*utp.PublishMessage{{Topic: "a.b", Payload: []byte("1")}, {Topic: "a.c", Payload: []byte("2"), Ttl: "10m"}}}, &pbx.Publish{})
	add("publish empty payload", "client", &utp.Publish{MessageID: 2, Messages: []*utp.PublishMessage{{Topic: "a.b"}}}, &pbx.Publish{})
	add("publish 64 KiB", "client", &utp.Publish{MessageID: 3, DeliveryMode: 1, Messages: []*utp.PublishMessage{{Topic: "a.big", Payload: big}}}, &pbx.Publish{})
	add("keygen request", "client", &utp.Publish{MessageID: 4, Messages: []*utp.PublishMessage{{Topic: "unitdb/keygen", Payload: []byte(`[{"topic":"teams.alpha.ch1","type":"rw"}]`)}}}, &pbx.Publish{})
	add("relay", "client", &utp.Relay{MessageID: 5, RelayRequests: []*utp.RelayRequest{{Topic: "a.b", Last: "1h"}, {Topic: "a.c"}}}, &pbx.Relay{})
	add("subscribe", "client", &utp.Subscribe{MessageID: 6, Subscriptions: []*utp.Subscription{{Topic: "key/a.b"}, {DeliveryMode: 1, Delay: 250, Topic: "a.*"}}}, &pbx.Subscribe{})
	add("unsubscribe", "client", &utp.Unsubscribe{MessageID: 7, Subscriptions: []*utp.Subscription{{Topic: "a.b"}}}, &pbx.Unsubscribe{})
	add("pingreq", "client", &utp.Pingreq{}, &pbx.PingRequest{})
	add("disconnect", "client", &utp.Disconnect{MessageID: 8}, &pbx.Disconnect{})
	add("receipt (client)", "client", ctrl(9, utp.PUBLISH, utp.RECEIPT, nil), &pbx.ControlMessage{})
	add("receive", "client", ctrl(10, utp.PUBLISH, utp.RECEIVE, nil), &pbx.ControlMessage{})

	// From the server.
	add("connack accepted", "server", ctrl(0, utp.CONNECT, utp.ACKNOWLEDGE, connack(0, 3, 1234)), &pbx.ControlMessage{})
	add("connack negative conn id", "server", ctrl(0, utp.CONNECT, utp.ACKNOWLEDGE, connack(0, 1, -5)), &pbx.ControlMessage{})
	add("connack refused", "server", ctrl(0, utp.CONNECT, utp.ACKNOWLEDGE, connack(2, 0, 0)), &pbx.ControlMessage{})
	add("publish acknowledge", "server", ctrl(1, utp.PUBLISH, utp.ACKNOWLEDGE, nil), &pbx.ControlMessage{})
	add("subscribe acknowledge", "server", ctrl(6, utp.SUBSCRIBE, utp.ACKNOWLEDGE, nil), &pbx.ControlMessage{})
	add("unsubscribe acknowledge", "server", ctrl(7, utp.UNSUBSCRIBE, utp.ACKNOWLEDGE, nil), &pbx.ControlMessage{})
	add("relay acknowledge", "server", ctrl(5, utp.RELAY, utp.ACKNOWLEDGE, nil), &pbx.ControlMessage{})
	add("ping acknowledge", "server", ctrl(0, utp.PINGREQ, utp.ACKNOWLEDGE, nil), &pbx.ControlMessage{})
	add("receipt (server)", "server", ctrl(65535, utp.PUBLISH, utp.RECEIPT, nil), &pbx.ControlMessage{})
	add("complete", "server", ctrl(65535, utp.PUBLISH, utp.COMPLETE, nil), &pbx.ControlMessage{})
	add("notify", "server", ctrl(11, utp.PUBLISH, utp.NOTIFY, nil), &pbx.ControlMessage{})
	add("publish delivered", "server", &utp.Publish{MessageID: 12, DeliveryMode: 1, Messages: []*utp.PublishMessage{{Topic: "groups.acme.chat", Payload: []byte("hi"), Ttl: "1h"}}}, &pbx.Publish{})
	add("error notice", "server", &utp.Publish{MessageID: 13, Messages: []*utp.PublishMessage{{Topic: "unitdb/error/", Payload: []byte(`{"status":400,"message":"The request was invalid or cannot be otherwise served."}`)}}}, &pbx.Publish{})
	add("keygen answer", "server", &utp.Publish{MessageID: 14, Messages: []*utp.PublishMessage{{Topic: "unitdb/keygen", Payload: []byte(`[{"id":"k1","status":200,"key":"KEY","topic":"teams.alpha.ch1","uuid":"7"}]`)}}}, &pbx.Publish{})

	// The stream: a connack, a delivery with a NOTIFY, a ping's answer.
	var s bytes.Buffer
	for _, fr := range f.Frames {
		switch fr.Name {
		case "connack accepted", "notify", "publish delivered", "publish 64 KiB", "ping acknowledge":
			if fr.Name == "publish 64 KiB" {
				continue // from a client; the stream is the server's
			}
			b, _ := hex.DecodeString(fr.Hex)
			s.Write(b)
			f.Stream.Frames = append(f.Stream.Frames, fr.Name)
		}
	}
	f.Stream.Hex = hex.EncodeToString(s.Bytes())

	got, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	if err := os.WriteFile(*out, got, 0o644); err != nil {
		t.Fatal(err)
	}
}

// protoJSON is m as protobuf JSON, compact and stable, as other runtimes
// write it (field json names, bytes base64, 64-bit integers as strings).
func protoJSON(t *testing.T, m proto.Message) json.RawMessage {
	t.Helper()
	b, err := protojson.MarshalOptions{}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	out, _ := json.Marshal(v) // protojson adds random spaces; this doesn't
	return out
}
