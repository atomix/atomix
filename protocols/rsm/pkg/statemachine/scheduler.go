// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

type Scheduler interface {
	Time() time.Time
	Delay(d time.Duration, f func()) CancelFunc
	Schedule(t time.Time, f func()) CancelFunc
}

type CancelFunc func()

func newScheduler() *stateMachineScheduler {
	scheduler := &stateMachineScheduler{
		scheduledTasks: list.New(),
	}
	scheduler.time.Store(time.UnixMilli(0))
	return scheduler
}

type stateMachineScheduler struct {
	scheduledTasks *list.List
	indexedTasks   sync.Map
	time           atomic.Value
}

func (s *stateMachineScheduler) Time() time.Time {
	return s.time.Load().(time.Time)
}

func (s *stateMachineScheduler) Delay(d time.Duration, f func()) CancelFunc {
	return s.Schedule(s.Time().Add(d), f)
}

func (s *stateMachineScheduler) Schedule(t time.Time, f func()) CancelFunc {
	task := &timeTask{
		scheduler: s,
		time:      t,
		callback:  f,
	}
	task.schedule()
	return task.cancel
}

// tick runs the scheduled time-based tasks
func (s *stateMachineScheduler) tick(time time.Time) {
	if time.After(s.Time()) {
		element := s.scheduledTasks.Front()
		if element != nil {
			for element != nil {
				task := element.Value.(*timeTask)
				if task.isRunnable(time) {
					next := element.Next()
					s.scheduledTasks.Remove(element)
					s.time.Store(task.time)
					task.run()
					element = next
				} else {
					break
				}
			}
		}
		s.time.Store(time)
	}
}

// time-based task
type timeTask struct {
	scheduler *stateMachineScheduler
	callback  func()
	time      time.Time
	elem      *list.Element
}

func (t *timeTask) schedule() {
	if t.scheduler.scheduledTasks.Len() == 0 {
		t.elem = t.scheduler.scheduledTasks.PushBack(t)
	} else {
		element := t.scheduler.scheduledTasks.Back()
		for element != nil {
			time := element.Value.(*timeTask).time
			if element.Value.(*timeTask).time.UnixNano() < time.UnixNano() {
				t.elem = t.scheduler.scheduledTasks.InsertAfter(t, element)
				return
			}
			element = element.Prev()
		}
		t.elem = t.scheduler.scheduledTasks.PushFront(t)
	}
}

func (t *timeTask) isRunnable(time time.Time) bool {
	return time.UnixNano() >= t.time.UnixNano()
}

func (t *timeTask) run() {
	t.callback()
}

func (t *timeTask) cancel() {
	if t.elem != nil {
		t.scheduler.scheduledTasks.Remove(t.elem)
	}
}
