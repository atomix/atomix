// SPDX-FileCopyrightText: 2015-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.protocols.raft.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Base request for all client requests.
 */
public abstract class AbstractRaftRequest implements RaftRequest {

  /**
   * Abstract request builder.
   *
   * @param <T> The builder type.
   * @param <U> The request type.
   */
  protected abstract static class Builder<T extends Builder<T, U>, U extends AbstractRaftRequest> implements RaftRequest.Builder<T, U> {

    /**
     * Validates the builder.
     */
    protected void validate() {
    }

    @Override
    public String toString() {
      return toStringHelper(this).toString();
    }
  }
}
