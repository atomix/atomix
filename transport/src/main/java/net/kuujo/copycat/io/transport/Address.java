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
package net.kuujo.copycat.io.transport;

import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.CopycatSerializable;
import net.kuujo.copycat.io.serializer.SerializeWith;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.util.Assert;

import java.net.InetSocketAddress;

/**
 * Network address.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@SerializeWith(id=299)
public class Address implements CopycatSerializable {
  private InetSocketAddress address;

  public Address() {
  }

  public Address(Address address) {
    this(Assert.notNull(address, "address").address);
  }

  public Address(String host, int port) {
    this(new InetSocketAddress(host, port));
  }

  public Address(InetSocketAddress address) {
    this.address = Assert.notNull(address, "address");
  }

  /**
   * Returns the address host.
   *
   * @return The address host.
   */
  public String host() {
    return address.getHostString();
  }

  /**
   * Returns the address port.
   *
   * @return The address port.
   */
  public int port() {
    return address.getPort();
  }

  /**
   * Returns the underlying address.
   *
   * @return The underlying address.
   */
  public InetSocketAddress socketAddress() {
    return address;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    serializer.writeObject(address, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    address = serializer.readObject(buffer);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Address && ((Address) object).address.equals(address);
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + address.toString().hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[%s]", getClass().getSimpleName(), address);
  }

}
