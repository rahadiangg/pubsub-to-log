package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"github.com/sirupsen/logrus"
)

// need set GOOGLE_CLIENT_CREDENTIALS before run

func pullMesgs(projectID, subID string) {
	ctx := context.Background()

	// client
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		// fmt.Errorf("Failed to create client: %v", err)
		logrus.Error(fmt.Sprintf("Failed to create client: %v", err))
	}
	defer client.Close()

	// sub
	sub := client.Subscription(subID)

	// comment this if want long running
	// ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	// defer cancel()

	var received int32
	err = sub.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		// fmt.Fprintf(w, "Got message: %q\n", string(m.Data))
		logrus.Info(fmt.Sprintf("Got message: %q", string(m.Data)))
		atomic.AddInt32(&received, 1)
		m.Ack()
	})

	if err != nil {
		// return fmt.Errorf("sub.Receive: %v", err)
		logrus.Error(fmt.Sprintf("sub.Receive: %v", err))
	}

	// fmt.Fprintf(w, "Received %d messagees\n", received)
	logrus.Info(fmt.Sprintf("Receive %d messages", received))

}

func init() {
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetOutput(os.Stdout)
}

func main() {

	pullMesgs("my-project", "my-topic")
}
