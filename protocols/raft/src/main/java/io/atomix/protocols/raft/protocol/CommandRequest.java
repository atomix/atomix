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

import io.atomix.primitive.operation.PrimitiveOperation;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Client command request.
 * <p>
 * Command requests are submitted by clients to the Raft cluster to commit commands to
 * the replicated state machine. Each command request must be associated with a registered
 * {@link #session()} and have a unique {@link #sequenceNumber()} number within that session. Commands will
 * be applied in the cluster in the order defined by the provided sequence number. Thus, sequence numbers
 * should never be skipped. In the event of a failure of a command request, the request should be resent
 * with the same sequence number. Commands are guaranteed to be applied in sequence order.
 * <p>
 * Command requests should always be submitted to the server to which the client is connected and will
 * be forwarded to the current cluster leader. In the event that no leader is available, the request
 * will fail and should be resubmitted by the client.
 */
public class CommandRequest extends OperationRequest {

  /**
   * Returns a new submit request builder.
   *
   * @return A new submit request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public CommandRequest(long session, long sequence, PrimitiveOperation operation) {
    super(session, sequence, operation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, sequence);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof CommandRequest) {
      CommandRequest request = (CommandRequest) object;
      return request.session == session
          && request.sequence == sequence
          && request.operation.equals(operation);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", session)
        .add("sequence", sequence)
        .add("operation", operation)
        .toString();
  }

  /**
   * Command request builder.
   */
  public static class Builder extends OperationRequest.Builder<Builder, CommandRequest> {
    @Override
    public CommandRequest build() {
      validate();
      return new CommandRequest(session, sequence, operation);
    }
  }
}
