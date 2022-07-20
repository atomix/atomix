// SPDX-FileCopyrightText: 2019-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package async

import (
	"sort"
	"sync"
)

// IterAsync executes the given function f up to n times concurrently.
// Each call is done in a separate goroutine. On each iteration, the function f
// will be called with a unique sequential index i such that the index can be
// used to reference an element in an array or slice. If an error is returned
// by the function f for any index, an error will be returned. Otherwise,
// a nil result will be returned once all function calls have completed.
func IterAsync(n int, f func(i int) error) error {
	wg := sync.WaitGroup{}
	asyncErrors := make(chan error, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(j int) {
			err := f(j)
			if err != nil {
				asyncErrors <- err
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(asyncErrors)
	}()

	for err := range asyncErrors {
		return err
	}
	return nil
}

// ExecuteAsync executes the given function f up to n times concurrently, populating
// the given results slice with the results of each function call.
// Each call is done in a separate goroutine. On each iteration, the function f
// will be called with a unique sequential index i such that the index can be
// used to reference an element in an array or slice. If an error is returned
// by the function f for any index, an error will be returned. Otherwise,
// a nil result will be returned once all function calls have completed.
func ExecuteAsync[T any](n int, f func(i int) (T, error)) ([]T, error) {
	wg := sync.WaitGroup{}
	asyncErrors := make(chan error, n)
	asyncResults := make(chan T, n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(j int) {
			result, err := f(j)
			if err != nil {
				asyncErrors <- err
			} else {
				asyncResults <- result
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(asyncErrors)
		close(asyncResults)
	}()

	for err := range asyncErrors {
		return nil, err
	}

	results := make([]T, 0, n)
	for result := range asyncResults {
		results = append(results, result)
	}
	return results, nil
}

// ExecuteOrderedAsync executes the given function f up to n times concurrently, populating
// the given results slice with the results of each function call.
// Each call is done in a separate goroutine. On each iteration, the function f
// will be called with a unique sequential index i such that the index can be
// used to reference an element in an array or slice. If an error is returned
// by the function f for any index, an error will be returned. Otherwise,
// a nil result will be returned once all function calls have completed.
func ExecuteOrderedAsync[T any](n int, f func(i int) (T, error)) ([]T, error) {
	wg := sync.WaitGroup{}
	asyncErrors := make(chan error, n)
	asyncResults := make(chan *asyncResult[T], n)

	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(j int) {
			result, err := f(j)
			if err != nil {
				asyncErrors <- err
			} else {
				asyncResults <- &asyncResult[T]{
					i:      j,
					result: result,
				}
			}
			wg.Done()
		}(i)
	}

	go func() {
		wg.Wait()
		close(asyncErrors)
		close(asyncResults)
	}()

	for err := range asyncErrors {
		return nil, err
	}

	sortedResults := make([]*asyncResult[T], 0, n)
	for result := range asyncResults {
		sortedResults = append(sortedResults, result)
	}

	sort.Slice(sortedResults, func(i, j int) bool {
		return sortedResults[i].i < sortedResults[j].i
	})

	results := make([]T, n)
	for i, result := range sortedResults {
		results[i] = result.result
	}

	return results, nil
}

type asyncResult[T any] struct {
	i      int
	result T
}
