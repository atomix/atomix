/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.resource.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

/**
 * Resource event wrapper.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ResourceEvent implements CatalystSerializable {
  private int eventId;
  private Object event;

  public ResourceEvent() {
  }

  public ResourceEvent(int eventId, Object event) {
    this.eventId = eventId;
    this.event = event;
  }

  /**
   * Returns the event ID.
   *
   * @return The event ID.
   */
  public int id() {
    return eventId;
  }

  /**
   * Returns the event value.
   *
   * @return The event value.
   */
  public Object event() {
    return event;
  }

  @Override
  public void writeObject(BufferOutput<?> output, Serializer serializer) {
    output.writeByte(eventId);
    serializer.writeObject(event, output);
  }

  @Override
  public void readObject(BufferInput<?> input, Serializer serializer) {
    eventId = input.readByte();
    event = serializer.readObject(input);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d, event=%s]", getClass().getSimpleName(), eventId, event);
  }

}
