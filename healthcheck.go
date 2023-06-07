// Copyright 2020 Kentaro Hibino. All rights reserved.
// Use of this source code is governed by a MIT license
// that can be found in the LICENSE file.

package asynq

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"time"

	"github.com/hibiken/asynq/internal/base"
	"github.com/hibiken/asynq/internal/log"
)

// healthchecker is responsible for pinging broker periodically
// and call user provided HeathCheckFunc with the ping result.
type healthchecker struct {
	logger *log.Logger
	broker base.Broker

	// sql database connection
	sqlDbs map[string]*sql.DB

	// channel to communicate back to the long running "healthchecker" goroutine.
	done chan struct{}

	// interval between healthchecks.
	interval time.Duration

	// function to call periodically.
	healthcheckFunc func(error)
}

type healthcheckerParams struct {
	logger          *log.Logger
	broker          base.Broker
	sqlDbs          map[string]*sql.DB
	interval        time.Duration
	healthcheckFunc func(error)
}

func newHealthChecker(params healthcheckerParams) *healthchecker {
	return &healthchecker{
		logger:          params.logger,
		broker:          params.broker,
		sqlDbs:          params.sqlDbs,
		done:            make(chan struct{}),
		interval:        params.interval,
		healthcheckFunc: params.healthcheckFunc,
	}
}

func (hc *healthchecker) shutdown() {
	//if hc.healthcheckFunc == nil {
	//	return
	//}

	hc.logger.Debug("Healthchecker shutting down...")
	// Signal the healthchecker goroutine to stop.
	hc.done <- struct{}{}
}

func (hc *healthchecker) set(key string, val []byte) {
	// send error message to broker
	hc.broker.Client().Set(context.Background(), key, val, time.Duration(0))
}

func (hc *healthchecker) start(wg *sync.WaitGroup) {
	//if hc.healthcheckFunc == nil {
	//	return
	//}

	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTimer(hc.interval)
		for {
			select {
			case <-hc.done:
				hc.logger.Debug("Healthchecker done")
				timer.Stop()
				return
			case <-timer.C:
				// comment redis (broker) ping (reason: move to asynqmon package)
				//err := hc.broker.Ping()
				errMsg := make(map[string]string)
				for name, sqlDb := range hc.sqlDbs {
					if err := sqlDb.Ping(); err != nil {
						errMsg[name] = err.Error()
					}
					//hc.healthcheckFunc(err)
				}
				marshal, _ := json.Marshal(errMsg)
				hc.set("healthcheck", marshal)
				timer.Reset(hc.interval)
			}
		}
	}()
}
