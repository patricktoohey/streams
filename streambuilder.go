package streams

import (
	"log"

	"github.com/nats-io/nats.go"
)

func BuildPullSubscriber(js nats.JetStreamContext, subjects string, subscriberGroup string, inflight int) *nats.Subscription {

	// Create Pull based consumer with maximum N inflight.
	// PullMaxWaiting defines the max inflight pull requests.
	log.Println("Opening subscription...")
	sub, errsub := js.PullSubscribe(subjects, subscriberGroup, nats.PullMaxWaiting(inflight))

	if errsub != nil {
		log.Fatal(errsub)
	}
	return sub
}

func BuildStream(server string, streamName string, subjects string) (*nats.Conn, nats.JetStreamContext) {

	nc, nerr := nats.Connect(server)
	if nerr != nil {
		log.Fatal(nerr)
	}

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	stream, err := js.StreamInfo(streamName)
	if err != nil {
		log.Println(err)
	}
	if stream == nil {
		log.Printf("creating stream %q and subjects %q", streamName, subjects)
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{subjects},
		})
		if err != nil {
			log.Println(err)
		}
	}

	return nc, js
}
