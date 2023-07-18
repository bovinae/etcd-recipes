// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package recipe

import (
	"context"
	"errors"

	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrEmptyQueue = errors.New("queue is empty")
)

// Queue implements a multi-reader, multi-writer distributed queue.
type Queue struct {
	client *v3.Client
	ctx    context.Context

	keyPrefix string
}

func NewQueue(client *v3.Client, keyPrefix string) *Queue {
	return &Queue{client, context.TODO(), keyPrefix}
}

func (q *Queue) Enqueue(val string) error {
	_, err := newUniqueKV(q.client, q.keyPrefix, val)
	return err
}

// Dequeue returns Enqueue()'d elements in FIFO order. If the
// queue is empty and blocking is true, Dequeue blocks until elements are available.
// If the queue is empty and blocking is false, Dequeue return ErrEmptyQueue.
func (q *Queue) Dequeue(blocking bool) (string, error) {
	// TODO: fewer round trips by fetching more than one key
	resp, err := q.client.Get(q.ctx, q.keyPrefix, v3.WithFirstRev()...)
	if err != nil {
		return "", err
	}

	kv, err := claimFirstKey(q.client, resp.Kvs)
	if err != nil {
		return "", err
	} else if kv != nil {
		return string(kv.Value), nil
	} else if resp.More {
		// missed some items, retry to read in more
		return q.Dequeue(blocking)
	}

	// nothing yet; non-blocking mode return ErrEmptyQueue
	if !blocking {
		return "", ErrEmptyQueue
	}

	// nothing yet; blocking mode wait on elements
	ev, err := WaitPrefixEvents(
		q.client,
		q.keyPrefix,
		resp.Header.Revision,
		[]mvccpb.Event_EventType{mvccpb.PUT})
	if err != nil {
		return "", err
	}

	ok, err := deleteRevKey(q.client, string(ev.Kv.Key), ev.Kv.ModRevision)
	if err != nil {
		return "", err
	} else if !ok {
		return q.Dequeue(blocking)
	}
	return string(ev.Kv.Value), err
}

func (q *Queue) Empty() (bool, error) {
	resp, err := q.client.Get(q.ctx, q.keyPrefix, v3.WithPrefix())
	if err != nil {
		return true, err
	}
	if len(resp.Kvs) == 0 {
		return true, nil
	}
	return false, nil
}
