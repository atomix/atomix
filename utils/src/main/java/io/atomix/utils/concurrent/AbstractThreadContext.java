// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.utils.concurrent;

/**
 * Abstract thread context.
 */
public abstract class AbstractThreadContext implements ThreadContext {
  private volatile boolean blocked;

  @Override
  public boolean isBlocked() {
    return blocked;
  }

  @Override
  public void block() {
    blocked = true;
  }

  @Override
  public void unblock() {
    blocked = false;
  }
}
