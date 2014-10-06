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

  public void setHost(String host) {
    this.host = host;
  }

  public String getHost() {
    return host;
  }

  public TcpMemberConfig withHost(String host) {
    this.host = host;
    return this;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getPort() {
    return port;
  }

  public TcpMemberConfig withPort(int port) {
    this.port = port;
    return this;
  }

  @Override
  public String toString() {
    return String.format("%s[host=%s, port=%d]", getClass().getSimpleName(), host, port);
  }

}
