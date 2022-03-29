// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.impl;

import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Pending command.
 */
public final class PendingCommand {
  private final CommandRequest request;
  private final CompletableFuture<CommandResponse> future;

  public PendingCommand(CommandRequest request, CompletableFuture<CommandResponse> future) {
    this.request = request;
    this.future = future;
  }

  /**
   * Returns the pending command request.
   *
   * @return the pending command request
   */
  public CommandRequest request() {
    return request;
  }

  /**
   * Returns the pending command future.
   *
   * @return the pending command future
   */
  public CompletableFuture<CommandResponse> future() {
    return future;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("request", request)
        .toString();
  }
}
