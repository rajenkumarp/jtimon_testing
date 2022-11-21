package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/nileshsimaria/jtimon/dialout"
	"github.com/nileshsimaria/jtimon/gnmi/gnmi"
	gnmi_ext1 "github.com/nileshsimaria/jtimon/gnmi/gnmi_ext"
	gnmi_juniper_header_ext "github.com/nileshsimaria/jtimon/gnmi/gnmi_juniper_header_ext"
)

var KafkaTopic = "gnmi-data"
var kafkaBrokers = []string{"10.32.20.88:29092"}
var InfluxHost = "10.111.86.18"
var InfluxPort = 8086

type JCtx struct {
	config Config
}
type PathsConfig struct {
	Path string `json:"path"`
	Freq uint64 `json:"freq"`
	Mode string `json:"mode"`
}
type Config struct {
	Port              int           `json:"port"`
	Host              string        `json:"host"`
	User              string        `json:"user"`
	Password          string        `json:"password"`
	CID               string        `json:"cid"`
	Meta              bool          `json:"meta"`
	EOS               bool          `json:"eos"`
	Influx            InfluxConfig  `json:"influx"`
	Kafka             *KafkaConfig  `json:"kafka"`
	Paths             []PathsConfig `json:"paths"`
	Log               LogConfig     `json:"log"`
	PasswordDecoder   string        `json:"password-decoder"`
	EnableUintSupport bool          `json:"enable-uint"`
}
type LogConfig struct {
	File          string `json:"file"`
	PeriodicStats int    `json:"periodic-stats"`
	Verbose       bool   `json:"verbose"`
	out           *os.File
	logger        *log.Logger
}
type InfluxConfig struct {
	Server               string `json:"server"`
	Port                 int    `json:"port"`
	Dbname               string `json:"dbname"`
	User                 string `json:"user"`
	Password             string `json:"password"`
	Recreate             bool   `json:"recreate"`
	Measurement          string `json:"measurement"`
	BatchSize            int    `json:"batchsize"`
	BatchFrequency       int    `json:"batchfrequency"`
	HTTPTimeout          int    `json:"http-timeout"`
	RetentionPolicy      string `json:"retention-policy"`
	AccumulatorFrequency int    `json:"accumulator-frequency"`
	WritePerMeasurement  bool   `json:"write-per-measurement"`
}
type KafkaConfig struct {
	Version            string   `json:"version"`
	Brokers            []string `json:"brokers"`
	ClientID           string   `json:"client-id"`
	Topic              string   `json:"topic"`
	CompressionCodec   int      `json:"compression-codec"`
	RequiredAcks       int      `json:"required-acks"`
	MaxRetry           int      `json:"max-retry"`
	MaxMessageBytes    int      `json:"max-message-bytes"`
	SASLUser           string   `json:"sasl-username"`
	SASLPass           string   `json:"sasl-password"`
	TLSCA              string   `json:"tls-ca"`
	TLSCert            string   `json:"tls-cert"`
	TLSKey             string   `json:"tls-key"`
	InsecureSkipVerify bool     `json:"insecure-skip-verify"`
	producer           *sarama.SyncProducer
}

func main() {
	var rspFromDevice *gnmi.SubscribeResponse

	jctx := &JCtx{
		config: Config{
			Host: "127.0.0.1",
			Port: 32767,
			Log: LogConfig{
				Verbose: true,
			},
			Influx: InfluxConfig{
				Server:      InfluxHost,
				Port:        InfluxPort,
				Dbname:      "TEST",
				Measurement: "jtisim",
				Recreate:    true,
				User:        "influx",
				Password:    "influxdb",
			},
		},
	}
	dialOutCfg := jctx.config
	cfgPayload, _ := json.Marshal(&dialOutCfg)

	var hdrInputExt = gnmi_juniper_header_ext.GnmiJuniperTelemetryHeaderExtension{
		SystemId: "my-device", ComponentId: 65535, SubComponentId: 0,
		SensorName: "sensor_1", SequenceNumber: 1, SubscribedPath: "/interfaces/",
		StreamedPath: "/interfaces/", Component: "mib2d",
	}

	hdrInputExtBytes, _ := proto.Marshal(&hdrInputExt)

	rspFromDevice = &gnmi.SubscribeResponse{
		Extension: []*gnmi_ext1.Extension{
			{
				Ext: &gnmi_ext1.Extension_RegisteredExt{
					RegisteredExt: &gnmi_ext1.RegisteredExtension{
						Id:  gnmi_ext1.ExtensionID_EID_JUNIPER_TELEMETRY_HEADER,
						Msg: hdrInputExtBytes,
					},
				},
			},
		},
		Response: &gnmi.SubscribeResponse_Update{
			Update: &gnmi.Notification{
				Timestamp: 1589476296083000000,
				Prefix: &gnmi.Path{
					Origin: "",
					Elem: []*gnmi.PathElem{
						{Name: "interfaces"},
						{Name: "interface", Key: map[string]string{"k1": "foo"}},
						{Name: "subinterfaces"},
						{Name: "subinterface", Key: map[string]string{"k1": "foo1", "k2": "bar1"}},
					},
				},
				Update: []*gnmi.Update{
					{
						Path: &gnmi.Path{
							Origin: "",
							Elem: []*gnmi.PathElem{
								{Name: "state"},
								{Name: "description"},
							},
						},
						Val: &gnmi.TypedValue{
							Value: &gnmi.TypedValue_StringVal{StringVal: "Hello"},
						},
					},
					{
						Path: &gnmi.Path{
							Origin: "",
							Elem: []*gnmi.PathElem{
								{Name: "state"},
								{Name: "mtu"},
							},
						},
						Val: &gnmi.TypedValue{
							Value: &gnmi.TypedValue_IntVal{IntVal: 1500},
						},
					},
				},
			},
		},
	}

	var rspString string
	rspString = fmt.Sprintf("%s", rspFromDevice)
	log.Printf("response from device: %v", rspString)
	var dialOutRsp dialout.DialOutResponse
	dialOutRsp.Device = "test_device"
	dialOutRsp.DialOutContext = cfgPayload
	dialOutRsp.Response = append(dialOutRsp.Response, rspFromDevice)
	payload, err := proto.Marshal(&dialOutRsp)
	cn := "127.0.0.1"
	if err != nil {
		log.Printf("[%v, DialOutSubscriber]: Marshalling failed for %s, len: %v\n", cn, rspString, len(rspString))
	}

	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(kafkaBrokers, config)

	msg := &sarama.ProducerMessage{Topic: KafkaTopic, Key: sarama.StringEncoder(cn), Value: sarama.ByteEncoder(payload)}
	producer.SendMessage(msg)
}
