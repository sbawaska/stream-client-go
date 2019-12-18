/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/cloudevents/sdk-go"
	"github.com/projectriff/stream-client-go/pkg/liiklus"
)

// StreamClient allows publishing to a riff stream, through a liiklus gateway. Events are published as CloudEvents
type StreamClient struct {
	// Gateway is the host:port of the liiklus gRPC endpoint.
	Gateway string
	// TopicName is the name of the liiklus topic backing the stream.
	TopicName string
	// acceptableContentType is the content type that the stream is able to persist. Incompatible content types will be rejected.
	acceptableContentType string
	// client is the gRPC client for the liiklus API.
	client liiklus.LiiklusServiceClient
	// conn is a reference to the underlying connection, kept for proper cleanup.
	conn *grpc.ClientConn
}

type PublishResult struct {
	Partition uint32
	Offset    uint64
}

// EventHandler is a function to process the messages read from the stream and is passed as
// a parameter to the subscribe call.
type EventHandler = func(ctx context.Context, payload io.Reader, contentType string) error

// EventErrHandler is a function to handle errors while reading subscription messages and
// is passed as a parameter to the subscribe call.
// This function may call the passed CancelFunc parameter to cancel the subscription
type EventErrHandler = func(cancel context.CancelFunc, err error)

// NewStreamClient creates a new StreamClient for a given stream.
func NewStreamClient(gateway string, topic string, acceptableContentType string) (*StreamClient, error) {
	timeout, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	conn, err := grpc.DialContext(timeout, gateway, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	client := liiklus.NewLiiklusServiceClient(conn)
	return &StreamClient{
		Gateway:               gateway,
		TopicName:             topic,
		acceptableContentType: acceptableContentType,
		client:                client,
		conn:                  conn,
	}, nil
}

func (lc *StreamClient) Publish(ctx context.Context, payload io.Reader, key io.Reader, contentType string, headers map[string]string) (PublishResult, error) {
	var err error

	event := cloudevents.NewEvent()
	event.SetID(fmt.Sprintf("scg-%d", time.Now().UnixNano()))
	if chopContentType(contentType) != chopContentType(lc.acceptableContentType) { // TODO support smarter compatibility (eg subtypes)
		return PublishResult{}, fmt.Errorf("contentType %q not compatible with expected contentType %q", contentType, lc.acceptableContentType)
	}
	err = event.Context.SetDataContentType(contentType)
	if err != nil {
		return PublishResult{}, err
	}
	if bytes2, err := ioutil.ReadAll(payload); err != nil {
		return PublishResult{}, err
	} else {
		err = event.SetData(bytes2)
		if err != nil {
			return PublishResult{}, err
		}
	}

	err = event.Validate()
	if err != nil {
		return PublishResult{}, err
	}
	var value []byte
	var kValue []byte
	if value, err = event.MarshalJSON(); err != nil {
		return PublishResult{}, err
	}
	if key != nil {
		if kValue, err = ioutil.ReadAll(key); err != nil {
			return PublishResult{}, err
		}
	}
	request := liiklus.PublishRequest{
		Topic: lc.TopicName,
		Value: value,
		Key:   kValue,
	}
	if publishReply, err := lc.client.Publish(ctx, &request); err != nil {
		return PublishResult{}, err
	} else {
		return PublishResult{Offset: publishReply.Offset, Partition: publishReply.Partition}, nil
	}
}

func chopContentType(contentType string) string {
	return strings.Split(contentType, ";")[0]
}

// Subscribe function should be used to listen for events from the StreamClient TopicName after the given offset. An offset of zero should be
// provided to read from the beginning. The provided EventHandler function will be called for each value.
// To deal with errors while reading messages, an error handler function should also be provided.
//
// The function returns a context.CancelFunc which may be called for cancelling the subscription.
func (lc *StreamClient) Subscribe(ctx context.Context, group string, offset uint64, f EventHandler, e EventErrHandler) (context.CancelFunc, error) {
	subContext, cancel := context.WithCancel(ctx)
	request := liiklus.SubscribeRequest{
		Topic:                lc.TopicName,
		Group:                group,
		AutoOffsetReset:      liiklus.SubscribeRequest_EARLIEST,
	}
	subscribedClient, err := lc.client.Subscribe(subContext, &request)
	if err != nil {
		return cancel, err
	}

	subscribeReply, err := subscribedClient.Recv()
	if err != nil {
		return cancel, err
	}

	receiveRequest := liiklus.ReceiveRequest{
		Assignment:           subscribeReply.GetAssignment(),
		LastKnownOffset:      offset,
	}
	receiveClient, err := lc.client.Receive(subContext, &receiveRequest)
	if err != nil {
		return cancel, err
	}

	go func() {
		for {
			select {
			case <- subContext.Done():
				e(cancel, errors.New("context terminated"))
				return
			default:
			}
			recvReply, err := receiveClient.Recv()
			if err != nil {
				e(cancel, err)
				return
			}

			m := cloudevents.NewEvent()

			record := recvReply.GetRecord()
			err = m.UnmarshalJSON(record.Value)
			if err != nil {
				e(cancel, err)
				return
			}
			payload, err := m.DataBytes()
			err = f(subContext, bytes.NewReader(payload), m.DataContentType())
			if err != nil {
				e(cancel, err)
				return
			}
			ackRequest := liiklus.AckRequest{
				Topic:                lc.TopicName,
				Group:                group,
				Offset:               record.Offset,
			}
			_, err = lc.client.Ack(subContext, &ackRequest)
			if err != nil {
				e(cancel, err)
				return
			}
		}
	}()

	return cancel, nil
}

// Close cleans up underlying resources used by this client. The client is then unable to publish.
func (lc *StreamClient) Close() error {
	return lc.conn.Close()
}
