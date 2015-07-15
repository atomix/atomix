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
package net.kuujo.copycat.cluster;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.Serialize;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;

import java.io.Serializable;

/**
 * Topic message.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Message<T> implements Serializable {
  private String topic;
  private int source;
  private Member member;
  private T body;

  private Message() {
  }

  public Message(String topic, int source, T body) {
    this.topic = topic;
    this.source = source;
    this.body = body;
  }

  /**
   * Returns the topic name.
   *
   * @return The topic name.
   */
  public String topic() {
    return topic;
  }

  /**
   * Returns the message source.
   */
  int source() {
    return source;
  }

  /**
   * Sets the message member.
   */
  Message<T> setMember(Member member) {
    this.member = member;
    return this;
  }

  /**
   * Returns the member from which the message was sent.
   *
   * @return The member from which the message was sent.
   */
  public Member member() {
    return member;
  }

  /**
   * Returns the message body.
   *
   * @return The message body.
   */
  public T body() {
    return body;
  }

  /**
   * Message serializer.
   */
  @Serialize(@Serialize.Type(id=410, type=Message.class))
  public static class Serializer implements net.kuujo.alleycat.Serializer<Message> {
    @Override
    public void write(Message message, BufferOutput buffer, Alleycat alleycat) {
      buffer.writeUTF8(message.topic);
      buffer.writeInt(message.source);
      alleycat.writeObject(message.body);
    }

    @Override
    public Message read(Class<Message> type, BufferInput buffer, Alleycat alleycat) {
      Message message = new Message();
      message.topic = buffer.readUTF8();
      message.source = buffer.readInt();
      message.body = alleycat.readObject(buffer);
      return message;
    }
  }

}
