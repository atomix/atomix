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
package net.kuujo.copycat.raft;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;

/**
 * Raft member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Member implements AlleycatSerializable {

  /**
   * Member type.
   */
  public static enum Type {

    /**
     * Represents an active voting member.
     */
    ACTIVE,

    /**
     * Represents a passive non-voting member.
     */
    PASSIVE,

    /**
     * Represents a pure client.
     */
    CLIENT

  }

  /**
   * Returns a new member builder.
   *
   * @return A new member builder.
   */
  public static Builder builder() {
    return new Builder(new Member());
  }

  private int id;
  private Type type = Type.ACTIVE;
  private long session;
  private String host;
  private int port;

  public Member() {
  }

  public Member(int id) {
    this.id = id;
  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  public int id() {
    return id;
  }

  /**
   * Returns the member type.
   *
   * @return The member type.
   */
  public Type type() {
    return type;
  }

  /**
   * Returns the member session ID.
   *
   * @return The member session ID.
   */
  public long session() {
    return session;
  }

  /**
   * Returns the member host.
   *
   * @return The member host.
   */
  public String host() {
    return host;
  }

  /**
   * Returns the member port.
   *
   * @return The member port.
   */
  public int port() {
    return port;
  }

  @Override
  public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    buffer.writeInt(id)
      .writeByte(type.ordinal())
      .writeString(host)
      .writeInt(port)
      .writeLong(session);
  }

  @Override
  public void readObject(BufferInput buffer, Alleycat alleycat) {
    id = buffer.readInt();
    type = Type.values()[buffer.readByte()];
    host = buffer.readString();
    port = buffer.readInt();
    session = buffer.readLong();
  }

  /**
   * Configures the member host and port.
   *
   * @param host The member host.
   * @param port The member port.
   */
  public void configure(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * Member builder.
   */
  public static class Builder implements net.kuujo.copycat.Builder<Member> {
    private final Member member;

    private Builder(Member member) {
      this.member = member;
    }

    /**
     * Sets the member ID.
     *
     * @param id The member ID.
     * @return The member builder.
     */
    public Builder withId(int id) {
      member.id = id;
      return this;
    }

    /**
     * Sets the member type.
     *
     * @param type The member type.
     * @return The member builder.
     */
    public Builder withType(Type type) {
      member.type = type;
      return this;
    }

    /**
     * Sets the member host.
     *
     * @param host The member host.
     * @return The member builder.
     */
    public Builder withHost(String host) {
      member.host = host;
      return this;
    }

    /**
     * Sets the member port.
     *
     * @param port The member port.
     * @return The member builder.
     */
    public Builder withPort(int port) {
      member.port = port;
      return this;
    }

    @Override
    public Member build() {
      return member;
    }
  }

}
