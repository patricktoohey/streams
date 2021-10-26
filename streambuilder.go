package streams

import (
	"log"

	"github.com/nats-io/nats.go"
)

func BuildStream(server string, streamName string, subjects string, subscriberGroup string) *nats.Subscription {
	nc, nerr := nats.Connect(server)
	if nerr != nil {
		log.Fatal(nerr)
	}
	defer nc.Close()

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

	// Create Pull based consumer with maximum 128 inflight.
	// PullMaxWaiting defines the max inflight pull requests.
	log.Println("Opening subscription...")
	sub, errsub := js.PullSubscribe(subjects, subscriberGroup, nats.PullMaxWaiting(128))

	if errsub != nil {
		log.Fatal(errsub)
	}
	return sub
}
