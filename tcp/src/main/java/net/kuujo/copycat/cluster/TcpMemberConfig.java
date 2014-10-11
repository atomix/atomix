/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.cluster;

/**
 * Basic TCP member configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TcpMemberConfig extends MemberConfig {
  private String host;
  private int port;

  public TcpMemberConfig() {
  }

  public TcpMemberConfig(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * Sets the member host.
   *
   * @param host The member host.
   */
  public void setHost(String host) {
    this.host = host;
  }

  /**
   * Returns the member host.
   *
   * @return The member host.
   */
  public String getHost() {
    return host;
  }

  /**
   * Sets the member host, returning the configuration for method chaining.
   *
   * @param host The member host.
   * @return The member configuration.
   */
  public TcpMemberConfig withHost(String host) {
    this.host = host;
    return this;
  }

  /**
   * Sets the member port.
   *
   * @param port The member port.
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Returns the member port.
   *
   * @return The member port.
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets the member port, returning the configuration for method chaining.
   *
   * @param port The member port.
   * @return The member configuration.
   */
  public TcpMemberConfig withPort(int port) {
    this.port = port;
    return this;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof TcpMemberConfig) {
      TcpMemberConfig member = (TcpMemberConfig) object;
      return member.host.equals(host) && member.port == port;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 13;
    hashCode = 37 * hashCode + getId().hashCode();
    hashCode = 37 * hashCode + host.hashCode();
    hashCode = 37 * hashCode + port;
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s[host=%s, port=%d]", getClass().getSimpleName(), host, port);
  }

}
