// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

import java.time.Duration;

/**
 * Null thread context.
 */
public class NullThreadContext implements ThreadContext {
  @Override
  public Scheduled schedule(Duration delay, Runnable callback) {
    return null;
  }

  @Override
  public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
    return null;
  }

  @Override
  public boolean isBlocked() {
    return false;
  }

  @Override
  public void block() {

  }

  @Override
  public void unblock() {

  }

  @Override
  public void close() {

  }

  @Override
  public void execute(Runnable command) {

  }
}
