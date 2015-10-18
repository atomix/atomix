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
package io.atomix.coordination;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;

/**
 * Message.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@SerializeWith(id=119)
public class Message implements CatalystSerializable {
  private String topic;
  private Object body;

  public Message() {
  }

  public Message(String topic, Object body) {
    this.topic = Assert.notNull(topic, "topic");
    this.body = body;
  }

  /**
   * Returns the message topic.
   *
   * @return The message topic.
   */
  public String topic() {
    return topic;
  }

  /**
   * Returns the message body.
   *
   * @return The message body.
   */
  public Object body() {
    return body;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeString(topic);
    serializer.writeObject(body, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    topic = buffer.readString();
    body = serializer.readObject(buffer);
  }

}
