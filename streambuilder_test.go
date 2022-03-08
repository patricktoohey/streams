package streams

import (
	"encoding/json"
	"log"
	"os"
	"testing"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func RunBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return natsserver.RunServer(&opts)
}

func shutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}

type Example struct {
	Name  string
	Number int
}

func Test_Serial(t *testing.T) {

	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	stream := `TEST_STREAM`
	subjects := `TEST_STREAM.*`
	group := `TEST_SUBSCRIBER`

	nc, js := BuildStream(s.ClientURL(), stream, subjects)
	c, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)

	sub := BuildPullSubscriber(js, subjects, group, 1)

	c.Publish("TEST_STREAM.Published", Example{Name: "Mark", Number: 11})

	m, err := sub.Fetch(1)

	if err != nil {
		t.Fail()
	}

	if len(m) != 1 {
		t.Fail()
	}

	msg := m[0].Data

	var received Example
	errUnmarshal := json.Unmarshal(msg, &received)

	if errUnmarshal != nil {
		log.Fatal(errUnmarshal)
	}

	if received.Name != "Mark" {
		t.Fail()
	}
	if received.Number != 11 {
		t.Fail()
	}

	sub.Unsubscribe()
	c.Close()
}
