/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.protocol;

import io.atomix.protocols.raft.RaftError;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base response for all client responses.
 */
public abstract class AbstractRaftResponse implements RaftResponse {
  protected final Status status;
  protected final RaftError error;

  protected AbstractRaftResponse(Status status, RaftError error) {
    this.status = status;
    this.error = error;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public RaftError error() {
    return error;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) return true;
    if (object == null || !getClass().isAssignableFrom(object.getClass())) return false;

    AbstractRaftResponse response = (AbstractRaftResponse) object;
    return response.status == status && Objects.equals(response.error, error);
  }

  @Override
  public String toString() {
    if (status == Status.OK) {
      return toStringHelper(this)
          .add("status", status)
          .toString();
    } else {
      return toStringHelper(this)
          .add("status", status)
          .add("error", error)
          .toString();
    }
  }

  /**
   * Abstract response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  protected static abstract class Builder<T extends Builder<T, U>, U extends AbstractRaftResponse> implements RaftResponse.Builder<T, U> {
    protected Status status;
    protected RaftError error;

    @Override
    @SuppressWarnings("unchecked")
    public T withStatus(Status status) {
      this.status = checkNotNull(status, "status cannot be null");
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withError(RaftError error) {
      this.error = checkNotNull(error, "error cannot be null");
      return (T) this;
    }

    /**
     * Validates the builder.
     */
    protected void validate() {
      checkNotNull(status, "status cannot be null");
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("status", status)
          .add("error", error)
          .toString();
    }
  }
}
