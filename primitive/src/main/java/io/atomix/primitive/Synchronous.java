// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.primitive;

import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.AtomixRuntimeException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * DistributedPrimitive that is a synchronous (blocking) version of
 * another.
 *
 * @param <T> type of DistributedPrimitive
 */
public abstract class Synchronous<T extends AsyncPrimitive> implements SyncPrimitive {

  private final T primitive;

  public Synchronous(T primitive) {
    this.primitive = primitive;
  }

  @Override
  public String name() {
    return primitive.name();
  }

  @Override
  public PrimitiveType type() {
    return primitive.type();
  }

  @Override
  public PrimitiveProtocol protocol() {
    return primitive.protocol();
  }

  @Override
  public void delete() {
    try {
      primitive.delete().get(DEFAULT_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AtomixRuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      primitive.close().get(DEFAULT_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new AtomixRuntimeException(e);
    }
  }
}
