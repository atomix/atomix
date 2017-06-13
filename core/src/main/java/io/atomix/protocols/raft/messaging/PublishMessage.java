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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Event publish request.
 * <p>
 * Publish requests are used by servers to publish event messages to clients. Event messages are
 * sequenced based on the point in the Raft log at which they were published to the client. The
 * {@link #eventIndex()} indicates the index at which the event was sent, and the {@link #previousIndex()}
 * indicates the index of the prior event messages sent to the client. Clients must ensure that event
 * messages are received in sequence by tracking the last index for which they received an event message
 * and validating {@link #previousIndex()} against that index.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PublishMessage extends SessionMessage {

  /**
   * Returns a new publish request builder.
   *
   * @return A new publish request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private final long eventIndex;
  private final long previousIndex;
  private final List<byte[]> events;

  public PublishMessage(long session, long eventIndex, long previousIndex, List<byte[]> events) {
    super(session);
    this.eventIndex = eventIndex;
    this.previousIndex = previousIndex;
    this.events = events;
  }

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  public long eventIndex() {
    return eventIndex;
  }

  /**
   * Returns the previous event index.
   *
   * @return The previous event index.
   */
  public long previousIndex() {
    return previousIndex;
  }

  /**
   * Returns the request events.
   *
   * @return The request events.
   */
  public List<byte[]> events() {
    return events;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, eventIndex, previousIndex, events);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PublishMessage) {
      PublishMessage request = (PublishMessage) object;
      return request.session == session
        && request.eventIndex == eventIndex
        && request.previousIndex == previousIndex
        && request.events.equals(events);
    }
    return false;
  }

  @Override
  public String toString() {
    return toStringHelper(this)
            .add("session", session)
            .add("eventIndex", eventIndex)
            .add("previousIndex", previousIndex)
            .add("events", events.stream().map(ArraySizeHashPrinter::of).collect(Collectors.toList()))
            .toString();
  }

  /**
   * Publish request builder.
   */
  public static class Builder extends SessionMessage.Builder<Builder, PublishMessage> {
    private long eventIndex;
    private long previousIndex;
    private List<byte[]> events;

    /**
     * Sets the event index.
     *
     * @param eventIndex The event index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code index} is less than 1
     */
    public Builder withEventIndex(long eventIndex) {
      checkArgument(eventIndex > 0, "eventIndex must be positive");
      this.eventIndex = eventIndex;
      return this;
    }

    /**
     * Sets the previous event index.
     *
     * @param previousIndex The previous event index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code index} is less than 1
     */
    public Builder withPreviousIndex(long previousIndex) {
      checkArgument(previousIndex >= 0, "previousIndex must be positive");
      this.previousIndex = previousIndex;
      return this;
    }

    /**
     * Sets the request events.
     *
     * @param events The request events.
     * @return The publish request builder.
     */
    public Builder withEvents(byte[]... events) {
      return withEvents(Arrays.asList(checkNotNull(events, "events cannot be null")));
    }

    /**
     * Sets the request events.
     *
     * @param events The request events.
     * @return The publish request builder.
     */
    public Builder withEvents(List<byte[]> events) {
      this.events = checkNotNull(events, "events cannot be null");
      return this;
    }

    @Override
    protected void validate() {
      super.validate();
      checkArgument(eventIndex > 0, "eventIndex must be positive");
      checkArgument(previousIndex >= 0, "previousIndex must be positive");
      checkNotNull(events, "events cannot be null");
    }

    /**
     * @throws IllegalStateException if sequence is less than 1 or message is null
     */
    @Override
    public PublishMessage build() {
      validate();
      return new PublishMessage(session, eventIndex, previousIndex, events);
    }
  }
}
