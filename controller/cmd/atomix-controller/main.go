// SPDX-FileCopyrightText: 2022-present Intel Corporation
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"github.com/atomix/runtime/controller/pkg/apis"
	"github.com/atomix/runtime/controller/pkg/controller/util/k8s"
	"github.com/atomix/runtime/pkg/logging"
	"os"
	"runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
)

var log = logging.GetLogger("atomix", "runtime")

func init() {
	logging.SetLevel(logging.DebugLevel)
}

func main() {
	log.Info(fmt.Sprintf("Go Version: %s", runtime.Version()))
	log.Info(fmt.Sprintf("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH))

	var namespace string
	if len(os.Args) > 1 {
		namespace = os.Args[1]
	}

	// Get a config to talk to the apiserver
	cfg, err := config.GetConfig()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// Become the leader before proceeding
	_ = k8s.BecomeLeader(context.TODO())

	r := k8s.NewFileReady()
	err = r.Set()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
	defer func() {
		_ = r.Unset()
	}()

	// Create a new Cmd to provide shared dependencies and start components
	mgr, err := manager.New(cfg, manager.Options{Namespace: namespace})
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// Setup Scheme for all resources
	if err := apis.AddToScheme(mgr.GetScheme()); err != nil {
		log.Error(err)
		os.Exit(1)
	}

	/*
		// Add all the controllers
		if err := corev2beta1.AddControllers(mgr); err != nil {
			log.Error(err)
			os.Exit(1)
		}
		if err := primitivesv2beta1.AddControllers(mgr); err != nil {
			log.Error(err)
			os.Exit(1)
		}
		if err := sidecarv2beta1.AddControllers(mgr); err != nil {
			log.Error(err)
			os.Exit(1)
		}
	*/

	// Start the manager
	log.Info("Starting the Manager")
	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		log.Error(err, "controller exited non-zero")
		os.Exit(1)
	}
}
