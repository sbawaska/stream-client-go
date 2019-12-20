package client_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go"
	client "github.com/projectriff/stream-client-go"
)

// This is an integration test meant to be run against a kafka custer. Please refer to the CI scripts for
// setup details
func TestSimplePublishSubscribe(t *testing.T) {
	now := time.Now()
	topic := fmt.Sprintf("test_%s%d%d%d", t.Name(), now.Hour(), now.Minute(), now.Second())

	c := setupStreamingClient(topic, t)

	payload := "FOO"
	err := publish(c, payload, "text/plain", t)
	if err != nil {
		t.Fatal(err)
	}
	subscribe(c, payload, 0, t)
}

func setupStreamingClient(topic string, t *testing.T) *client.StreamClient {
	c, err := client.NewStreamClient("localhost:6565", topic, "text/plain")
	if err != nil {
		t.Error(err)
	}
	return c
}

func publish(c *client.StreamClient, value, contentType string, t *testing.T) error {
	reader := strings.NewReader(value)
	publishResult, err := c.Publish(context.Background(), "test", reader, nil, contentType)
	if err != nil {
		return err
	}
	fmt.Printf("Published: %+v\n", publishResult)
	return nil
}

func subscribe(c *client.StreamClient, expectedValue string, offset uint64, t *testing.T) {

	var errHandler client.EventErrHandler
	errHandler = func(cancel context.CancelFunc, err error) {
		fmt.Printf("cancelling subsctiber due to: %v", err)
		cancel()
	}

	payloadChan := make(chan string)

	var eventHandler client.EventHandler
	eventHandler = func(ctx context.Context, event cloudevents.Event) error {

		payload, err := event.DataBytes()
		if err != nil {
			return err
		}
		payloadChan <- string(payload)
		return nil
	}

	_, err := c.Subscribe(context.Background(), "g8", offset, eventHandler, errHandler)
	if err != nil {
		t.Error(err)
	}
	v1 := <-payloadChan
	if v1 != expectedValue {
		t.Errorf("expected value: %s, but was: %s", expectedValue, v1)
	}
}

func TestSubscribeBeforePublish(t *testing.T) {
	now := time.Now()
	topic := fmt.Sprintf("test_%s%d%d%d", t.Name(), now.Hour(), now.Minute(), now.Second())

	c, err := client.NewStreamClient("localhost:6565", topic, "text/plain")
	if err != nil {
		t.Error(err)
	}

	testVal := "BAR"
	result := make(chan string)

	var eventHandler client.EventHandler
	eventHandler = func(ctx context.Context, event cloudevents.Event) error {
		bytes, err := event.DataBytes()
		if err != nil {
			return err
		}
		result <- string(bytes)
		return nil
	}
	var eventErrHandler client.EventErrHandler
	eventErrHandler = func(cancel context.CancelFunc, err error) {
		t.Error("Did not expect an error")
	}
	_, err = c.Subscribe(context.Background(), t.Name(), 0, eventHandler, eventErrHandler)
	if err != nil {
		t.Error(err)
	}
	err = publish(c, testVal, "text/plain", t)
	if err != nil {
		t.Fatal(err)
	}
	v1 := <- result
	if v1 != testVal {
		t.Errorf("expected value: %s, but was: %s", testVal, v1)
	}
}

func TestSubscribeCancel(t *testing.T) {
	now := time.Now()
	topic := fmt.Sprintf("test_%s%d%d%d", t.Name(), now.Hour(), now.Minute(), now.Second())

	c, err := client.NewStreamClient("localhost:6565", topic, "text/plain")
	if err != nil {
		t.Error(err)
	}

	expectedError := "expected_error"
	result := make(chan string)

	var eventHandler client.EventHandler
	eventHandler = func(ctx context.Context, event cloudevents.Event) error {
		bytes, err := event.DataBytes()
		if err != nil {
			return err
		}
		result <- string(bytes)
		return nil
	}
	var eventErrHandler client.EventErrHandler
	eventErrHandler = func(cancel context.CancelFunc, err error) {
		result <- expectedError
	}
	cancel, err := c.Subscribe(context.Background(), t.Name(), 0, eventHandler, eventErrHandler)
	if err != nil {
		t.Error(err)
	}
	cancel()
	v1 := <- result
	if v1 != expectedError {
		t.Errorf("expected value: %s, but was: %s", expectedError, v1)
	}
}

func TestMultipleSubscribe(t *testing.T) {
	now := time.Now()
	topic1 := fmt.Sprintf("test1_%s%d%d%d", t.Name(), now.Hour(), now.Minute(), now.Second())
	topic2 := fmt.Sprintf("test2_%s%d%d%d", t.Name(), now.Hour(), now.Minute(), now.Second())

	c1 := setupStreamingClient(topic1, t)
	c2 := setupStreamingClient(topic2, t)

	testVal1 := "BAR1"
	testVal2 := "BAR2"
	result1 := make(chan string)
	result2 := make(chan string)

	var eventErrHandler client.EventErrHandler
	eventErrHandler = func(cancel context.CancelFunc, err error) {
		panic(err)
	}
	var err error
	_, err = c1.Subscribe(context.Background(), t.Name()+"1", 0, func(ctx context.Context, event cloudevents.Event) error {
		bytes, err := event.DataBytes()
		if err != nil {
			return err
		}
		result1 <- string(bytes)
		return nil
	}, eventErrHandler)
	if err != nil {
		t.Error(err)
	}
	_, err = c2.Subscribe(context.Background(), t.Name()+"2", 0, func(ctx context.Context, event cloudevents.Event) error {
		bytes, err := event.DataBytes()
		if err != nil {
			return err
		}
		result2 <- string(bytes)
		return nil
	}, eventErrHandler)
	if err != nil {
		t.Error(err)
	}
	err = publish(c1, testVal1, "text/plain", t)
	if err != nil {
		t.Fatal(err)
	}
	err = publish(c2, testVal2, "text/plain", t)
	if err != nil {
		t.Fatal(err)
	}

	v1 := <-result1
	if v1 != testVal1 {
		t.Errorf("expected value: %s, but was: %s", testVal1, v1)
	}
	v2 := <-result2
	if v2 != testVal2 {
		t.Errorf("expected value: %s, but was: %s", testVal2, v2)
	}
}
