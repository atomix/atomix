/*
 * Copyright 2017-present Open Networking Foundation
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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Close session request.
 */
public class CloseSessionRequest extends SessionRequest {

  /**
   * Returns a new unregister request builder.
   *
   * @return A new unregister request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final boolean delete;

  public CloseSessionRequest(long session, boolean delete) {
    super(session);
    this.delete = delete;
  }

  /**
   * Returns whether this request is a delete.
   *
   * @return indicates whether this is a delete request
   */
  public boolean delete() {
    return delete;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || !getClass().isAssignableFrom(object.getClass())) {
      return false;
    }

    CloseSessionRequest request = (CloseSessionRequest) object;
    return request.session == session && request.delete == delete;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .add("delete", delete)
        .toString();
  }

  /**
   * Close session request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, CloseSessionRequest> {
    private boolean delete;

    /**
     * Sets whether the request is a delete.
     *
     * @param delete whether the request is a delete
     * @return the request builder
     */
    public Builder withDelete(boolean delete) {
      this.delete = delete;
      return this;
    }

    @Override
    public CloseSessionRequest build() {
      validate();
      return new CloseSessionRequest(session, delete);
    }
  }
}
