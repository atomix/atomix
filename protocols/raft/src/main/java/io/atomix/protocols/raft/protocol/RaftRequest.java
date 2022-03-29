// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

/**
 * Base interface for requests.
 */
public interface RaftRequest extends RaftMessage {

  /**
   * Request builder.
   *
   * @param <T> The builder type.
   */
  interface Builder<T extends Builder<T, U>, U extends RaftRequest> extends io.atomix.utils.Builder<U> {
  }
}
