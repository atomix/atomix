// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/atomix/atomix/api/errors"
	runtimev1 "github.com/atomix/atomix/api/runtime/v1"
	protocol "github.com/atomix/atomix/protocols/rsm/api/v1"
	"github.com/bits-and-blooms/bloom/v3"
	"google.golang.org/grpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const chanBufSize = 1000

// The false positive rate for request/response filters
const fpRate float64 = 0.05

const defaultSessionTimeout = 1 * time.Minute

func newSessionClient(id protocol.SessionID, partition *PartitionClient, conn *grpc.ClientConn, timeout time.Duration) *SessionClient {
	session := &SessionClient{
		sessionID:  id,
		partition:  partition,
		conn:       conn,
		requestNum: &atomic.Uint64{},
		requestCh:  make(chan sessionRequestEvent, chanBufSize),
		ticker:     time.NewTicker(timeout / 4),
	}
	session.recorder = &Recorder{
		session: session,
	}
	session.open()
	return session
}

type SessionClient struct {
	sessionID  protocol.SessionID
	partition  *PartitionClient
	conn       *grpc.ClientConn
	ticker     *time.Ticker
	lastIndex  *sessionIndex
	requestNum *atomic.Uint64
	requestCh  chan sessionRequestEvent
	primitives sync.Map
	mu         sync.Mutex
	recorder   *Recorder
}

func (s *SessionClient) CreatePrimitive(ctx context.Context, meta runtimev1.PrimitiveMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.primitives.Load(meta.Name); ok {
		return nil
	}

	primitive := newPrimitiveClient(s, protocol.PrimitiveSpec{
		Type: protocol.PrimitiveType{
			Name:       meta.Type.Name,
			APIVersion: meta.Type.APIVersion,
		},
		PrimitiveName: protocol.PrimitiveName{
			Name: meta.Name,
		},
	})
	if err := primitive.open(ctx); err != nil {
		return err
	}
	s.primitives.Store(meta.Name, primitive)
	return nil
}

func (s *SessionClient) GetPrimitive(name string) (*PrimitiveClient, error) {
	primitive, ok := s.primitives.Load(name)
	if !ok {
		return nil, errors.NewUnavailable("primitive not found")
	}
	return primitive.(*PrimitiveClient), nil
}

func (s *SessionClient) ClosePrimitive(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	primitive, ok := s.primitives.LoadAndDelete(name)
	if !ok {
		return nil
	}
	if err := primitive.(*PrimitiveClient).close(ctx); err != nil {
		return err
	}
	return nil
}

func (s *SessionClient) nextRequestNum() protocol.SequenceNum {
	return protocol.SequenceNum(s.requestNum.Add(1))
}

func (s *SessionClient) open() {
	s.lastIndex = &sessionIndex{}
	s.lastIndex.Update(protocol.Index(s.sessionID))

	go func() {
		var nextRequestNum protocol.SequenceNum = 1
		requests := make(map[protocol.SequenceNum]bool)
		pendingRequests := make(map[protocol.SequenceNum]bool)
		responseStreams := make(map[protocol.SequenceNum]*sessionResponseStream)
		for {
			select {
			case requestEvent, ok := <-s.requestCh:
				if !ok {
					break
				}
				switch requestEvent.eventType {
				case sessionRequestEventStart:
					if requestEvent.requestNum == nextRequestNum {
						requests[nextRequestNum] = true
						log.Debugf("Started request %d", nextRequestNum)
						nextRequestNum++
						_, nextRequestPending := pendingRequests[nextRequestNum]
						for nextRequestPending {
							delete(pendingRequests, nextRequestNum)
							requests[nextRequestNum] = true
							nextRequestNum++
							_, nextRequestPending = pendingRequests[nextRequestNum]
						}
					} else {
						pendingRequests[requestEvent.requestNum] = true
					}
				case sessionRequestEventEnd:
					if requests[requestEvent.requestNum] {
						delete(requests, requestEvent.requestNum)
						log.Debugf("Finished request %d", requestEvent.requestNum)
					}
				case sessionStreamEventOpen:
					responseStreams[requestEvent.requestNum] = &sessionResponseStream{}
					log.Debugf("Opened request %d response stream", requestEvent.requestNum)
				case sessionStreamEventReceive:
					responseStream, ok := responseStreams[requestEvent.requestNum]
					if ok {
						if requestEvent.responseNum == responseStream.currentResponseNum+1 {
							responseStream.currentResponseNum++
							log.Debugf("Received request %d stream response %d", requestEvent.requestNum, requestEvent.responseNum)
						}
					}
				case sessionStreamEventClose:
					delete(responseStreams, requestEvent.requestNum)
					log.Debugf("Closed request %d response stream", requestEvent.requestNum)
				case sessionStreamEventAck:
					responseStream, ok := responseStreams[requestEvent.requestNum]
					if ok {
						if requestEvent.responseNum > responseStream.ackedResponseNum {
							responseStream.ackedResponseNum = requestEvent.responseNum
							log.Debugf("Acked request %d stream responses up to %d", requestEvent.requestNum, requestEvent.responseNum)
						}
					}
				}
			case <-s.ticker.C:
				openRequests := bloom.NewWithEstimates(uint(len(requests)), fpRate)
				for requestNum := range requests {
					requestBytes := make([]byte, 8)
					binary.BigEndian.PutUint64(requestBytes, uint64(requestNum))
					openRequests.Add(requestBytes)
				}
				completeResponses := make(map[protocol.SequenceNum]protocol.SequenceNum)
				for requestNum, responseStream := range responseStreams {
					if responseStream.currentResponseNum > 1 && responseStream.currentResponseNum > responseStream.ackedResponseNum {
						completeResponses[requestNum] = responseStream.currentResponseNum
					}
				}
				go func(lastRequestNum protocol.SequenceNum) {
					err := s.keepAliveSessions(context.Background(), lastRequestNum, openRequests, completeResponses)
					if err != nil {
						log.Error(err)
					} else {
						for requestNum, responseNum := range completeResponses {
							s.requestCh <- sessionRequestEvent{
								eventType:   sessionStreamEventAck,
								requestNum:  requestNum,
								responseNum: responseNum,
							}
						}
					}
				}(nextRequestNum - 1)
			}
		}
	}()
}

func (s *SessionClient) keepAliveSessions(ctx context.Context, lastRequestNum protocol.SequenceNum, openRequests *bloom.BloomFilter, completeResponses map[protocol.SequenceNum]protocol.SequenceNum) error {
	openRequestsBytes, err := json.Marshal(openRequests)
	if err != nil {
		return err
	}

	request := &protocol.KeepAliveRequest{
		Headers: &protocol.PartitionRequestHeaders{
			PartitionID: s.partition.id,
		},
		KeepAliveInput: &protocol.KeepAliveInput{
			SessionID:              s.sessionID,
			LastInputSequenceNum:   lastRequestNum,
			InputFilter:            openRequestsBytes,
			LastOutputSequenceNums: completeResponses,
		},
	}

	client := protocol.NewPartitionClient(s.conn)
	response, err := client.KeepAlive(ctx, request)
	if err != nil {
		if errors.IsFault(err) {
			log.Error("Detected potential data loss: ", err)
			log.Infof("Exiting process...")
			os.Exit(errors.Code(err))
		}
		return errors.NewInternal(err.Error())
	}
	s.lastIndex.Update(response.Headers.Index)
	return nil
}

func (s *SessionClient) close(ctx context.Context) error {
	close(s.requestCh)
	s.ticker.Stop()

	request := &protocol.CloseSessionRequest{
		Headers: &protocol.PartitionRequestHeaders{
			PartitionID: s.partition.id,
		},
		CloseSessionInput: &protocol.CloseSessionInput{
			SessionID: s.sessionID,
		},
	}

	client := protocol.NewPartitionClient(s.conn)
	_, err := client.CloseSession(ctx, request)
	if err != nil {
		return err
	}
	return nil
}

type Recorder struct {
	session *SessionClient
}

func (r *Recorder) Start(sequenceNum protocol.SequenceNum) {
	r.session.requestCh <- sessionRequestEvent{
		eventType:  sessionRequestEventStart,
		requestNum: sequenceNum,
	}
}

func (r *Recorder) StreamOpen(headers *protocol.ProposalRequestHeaders) {
	r.session.requestCh <- sessionRequestEvent{
		eventType:  sessionStreamEventOpen,
		requestNum: headers.SequenceNum,
	}
}

func (r *Recorder) StreamReceive(request *protocol.ProposalRequestHeaders, response *protocol.ProposalResponseHeaders) {
	r.session.requestCh <- sessionRequestEvent{
		eventType:   sessionStreamEventReceive,
		requestNum:  request.SequenceNum,
		responseNum: response.OutputSequenceNum,
	}
}

func (r *Recorder) StreamClose(headers *protocol.ProposalRequestHeaders) {
	r.session.requestCh <- sessionRequestEvent{
		eventType:  sessionStreamEventClose,
		requestNum: headers.SequenceNum,
	}
}

func (r *Recorder) End(sequenceNum protocol.SequenceNum) {
	r.session.requestCh <- sessionRequestEvent{
		eventType:  sessionRequestEventEnd,
		requestNum: sequenceNum,
	}
}

type sessionIndex struct {
	value uint64
}

func (i *sessionIndex) Update(index protocol.Index) {
	update := uint64(index)
	for {
		current := atomic.LoadUint64(&i.value)
		if current < update {
			updated := atomic.CompareAndSwapUint64(&i.value, current, update)
			if updated {
				break
			}
		} else {
			break
		}
	}
}

func (i *sessionIndex) Get() protocol.Index {
	value := atomic.LoadUint64(&i.value)
	return protocol.Index(value)
}

type sessionRequestEventType int

const (
	sessionRequestEventStart sessionRequestEventType = iota
	sessionRequestEventEnd
	sessionStreamEventOpen
	sessionStreamEventReceive
	sessionStreamEventClose
	sessionStreamEventAck
)

type sessionRequestEvent struct {
	requestNum  protocol.SequenceNum
	responseNum protocol.SequenceNum
	eventType   sessionRequestEventType
}

type sessionResponseStream struct {
	currentResponseNum protocol.SequenceNum
	ackedResponseNum   protocol.SequenceNum
}
