// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"github.com/atomix/atomix/runtime/pkg/logging"
	"github.com/atomix/atomix/runtime/pkg/utils/async"
	"github.com/atomix/go-sdk/pkg/atomix"
	"github.com/atomix/go-sdk/pkg/types"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"math/rand"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

var log = logging.GetLogger()

func init() {
	logging.SetLevel(logging.InfoLevel)
}

func main() {
	cmd := &cobra.Command{
		Use: "atomix-go-sdk-bench",
		Run: func(cmd *cobra.Command, args []string) {
			concurrency, err := cmd.Flags().GetInt("concurrency")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			writePercentage, err := cmd.Flags().GetFloat32("write-percentage")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			numKeys, err := cmd.Flags().GetInt("num-keys")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}
			sampleInterval, err := cmd.Flags().GetDuration("sample-interval")
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			if writePercentage > 1 {
				panic("writePercentage must be a decimal value between 0 and 1")
			}

			log.Infof("Starting benchmark...")
			log.Infof("concurrency: %d", concurrency)
			log.Infof("writePercentage: %f", writePercentage)
			log.Infof("numKeys: %d", numKeys)
			log.Infof("sampleInterval: %s", sampleInterval)

			m, err := atomix.Map[string, string]("test").
				Codec(types.Scalar[string]()).
				Get(context.Background())
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			keys := make([]string, numKeys)
			for i := 0; i < numKeys; i++ {
				keys[i] = uuid.New().String()
			}

			err = async.IterAsync(numKeys, func(i int) error {
				_, err := m.Put(context.Background(), keys[i], uuid.New().String())
				return err
			})
			if err != nil {
				fmt.Fprintln(cmd.OutOrStderr(), err.Error())
				os.Exit(1)
			}

			opCount := &atomic.Uint64{}
			totalDuration := &atomic.Int64{}
			for i := 0; i < concurrency; i++ {
				go func() {
					for {
						if rand.Intn(100) < int(writePercentage*100) {
							start := time.Now()
							if _, err := m.Put(context.Background(), keys[rand.Intn(numKeys)], keys[rand.Intn(numKeys)]); err != nil {
								log.Warn(err)
							}
							totalDuration.Add(int64(time.Now().Sub(start)))
						} else {
							start := time.Now()
							if _, err := m.Get(context.Background(), keys[rand.Intn(numKeys)]); err != nil {
								log.Warn(err)
							}
							totalDuration.Add(int64(time.Now().Sub(start)))
						}
						opCount.Add(1)
					}
				}()
			}

			// Wait for an interrupt signal
			signalCh := make(chan os.Signal, 2)
			signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

			ticker := time.NewTicker(10 * time.Second)
			for {
				select {
				case <-ticker.C:
					count := opCount.Swap(0)
					duration := totalDuration.Swap(0)
					if count > 0 {
						log.Infof("Completed %d operations in %s (~%s/request)", count, sampleInterval, time.Duration(duration/int64(count)))
					}
				case <-signalCh:
					return
				}
			}
		},
	}
	cmd.Flags().IntP("concurrency", "c", 100, "the number of concurrent operations to run")
	cmd.Flags().Float32P("write-percentage", "w", .5, "the percentage of operations to perform as writes")
	cmd.Flags().IntP("num-keys", "k", 1000, "the number of unique map keys to use")
	cmd.Flags().DurationP("sample-interval", "i", 10*time.Second, "the interval at which to sample performance")

	if err := cmd.Execute(); err != nil {
		panic(err)
	}
}
