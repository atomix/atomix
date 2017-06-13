/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.protocols.raft.messaging;

import io.atomix.util.ArraySizeHashPrinter;

import java.util.Arrays;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Session keep alive request.
 * <p>
 * Keep alive requests are sent by clients to servers to maintain a session registered via
 * a {@link OpenSessionMessage}. Once a session has been registered, clients are responsible for sending
 * keep alive requests to the cluster at a rate less than the provided {@link OpenSessionReply#timeout()}.
 * Keep alive requests also server to acknowledge the receipt of responses and events by the client.
 * The {@link #commandSequences()} number indicates the highest command sequence number for which the client
 * has received a response, and the {@link #eventIndexes()} numbers indicate the highest index for which the
 * client has received an event in proper sequence.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeepAliveMessage extends AbstractRaftMessage {

  /**
   * Returns a new keep alive request builder.
   *
   * @return A new keep alive request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final long[] sessionIds;
  private final long[] commandSequences;
  private final long[] eventIndexes;
  private final long[] connections;

  public KeepAliveMessage(long[] sessionIds, long[] commandSequences, long[] eventIndexes, long[] connections) {
    this.sessionIds = sessionIds;
    this.commandSequences = commandSequences;
    this.eventIndexes = eventIndexes;
    this.connections = connections;
  }

  /**
   * Returns the session identifiers.
   *
   * @return The session identifiers.
   */
  public long[] sessionIds() {
    return sessionIds;
  }

  /**
   * Returns the command sequence numbers.
   *
   * @return The command sequence numbers.
   */
  public long[] commandSequences() {
    return commandSequences;
  }

  /**
   * Returns the event indexes.
   *
   * @return The event indexes.
   */
  public long[] eventIndexes() {
    return eventIndexes;
  }

  /**
   * Returns the session connections.
   *
   * @return The session connections.
   */
  public long[] connections() {
    return connections;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), sessionIds, commandSequences, eventIndexes, connections);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof KeepAliveMessage) {
      KeepAliveMessage request = (KeepAliveMessage) object;
      return Arrays.equals(request.sessionIds, sessionIds)
        && Arrays.equals(request.commandSequences, commandSequences)
        && Arrays.equals(request.eventIndexes, eventIndexes)
        && Arrays.equals(request.connections, connections);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
            .add("sessionIds", ArraySizeHashPrinter.of(sessionIds))
            .add("commandSequences", ArraySizeHashPrinter.of(commandSequences))
            .add("eventIndexes", ArraySizeHashPrinter.of(eventIndexes))
            .add("connections", ArraySizeHashPrinter.of(connections))
            .toString();
  }

  /**
   * Keep alive request builder.
   */
  public static class Builder extends AbstractRaftMessage.Builder<Builder, KeepAliveMessage> {
    private long[] sessionIds;
    private long[] commandSequences;
    private long[] eventIndexes;
    private long[] connections;

    /**
     * Sets the session identifiers.
     *
     * @param sessionIds The session identifiers.
     * @return The request builders.
     * @throws NullPointerException if {@code sessionIds} is {@code null}
     */
    public Builder withSessionIds(long[] sessionIds) {
      this.sessionIds = checkNotNull(sessionIds, "sessionIds cannot be null");
      return this;
    }

    /**
     * Sets the command sequence numbers.
     *
     * @param commandSequences The command sequence numbers.
     * @return The request builder.
     * @throws NullPointerException if {@code commandSequences} is {@code null}
     */
    public Builder withCommandSequences(long[] commandSequences) {
      this.commandSequences = checkNotNull(commandSequences, "commandSequences cannot be null");
      return this;
    }

    /**
     * Sets the event indexes.
     *
     * @param eventIndexes The event indexes.
     * @return The request builder.
     * @throws NullPointerException if {@code eventIndexes} is {@code null}
     */
    public Builder withEventIndexes(long[] eventIndexes) {
      this.eventIndexes = checkNotNull(eventIndexes, "eventIndexes cannot be null");
      return this;
    }

    /**
     * Sets the client connections.
     *
     * @param connections The client connections.
     * @return The request builder.
     * @throws NullPointerException if {@code connections} is {@code null}
     */
    public Builder withConnections(long[] connections) {
      this.connections = checkNotNull(connections, "connections cannot be null");
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      this.sessionIds = checkNotNull(sessionIds, "sessionIds cannot be null");
      this.commandSequences = checkNotNull(commandSequences, "commandSequences cannot be null");
      this.eventIndexes = checkNotNull(eventIndexes, "eventIndexes cannot be null");
      this.connections = checkNotNull(connections, "connections cannot be null");
    }

    @Override
    public KeepAliveMessage build() {
      validate();
      return new KeepAliveMessage(sessionIds, commandSequences, eventIndexes, connections);
    }
  }
}
