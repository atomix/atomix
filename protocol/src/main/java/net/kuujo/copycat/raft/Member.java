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

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.CopycatSerializable;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.ConfigurationException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Configuration for connecting to a member of the Raft cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Member implements CopycatSerializable {
  private int id;
  private InetSocketAddress address;

  public Member() {
  }

  public Member(int id, String host, int port) {
    if (host == null)
      throw new NullPointerException("host cannot be null");
    this.id = id;
    try {
      this.address = new InetSocketAddress(InetAddress.getByName(host), port);
    } catch (UnknownHostException e) {
      throw new ConfigurationException(e);
    }
  }

  public Member(int id, InetSocketAddress address) {
    if (address == null)
      throw new NullPointerException("address cannot be null");
    this.id = id;
    this.address = address;
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
   * Returns the member address.
   *
   * @return The member address.
   */
  public InetSocketAddress address() {
    return address;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeInt(id);
    serializer.writeObject(address, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    id = buffer.readInt();
    address = serializer.readObject(buffer);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Member) {
      Member member = (Member) object;
      return member.id == id && member.address.equals(address);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + id;
    hashCode = 37 * hashCode + address.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d, address=%s]", getClass().getSimpleName(), id, address);
  }

}
