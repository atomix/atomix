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
 * Basic HTTP member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class HttpMember extends Member {
  private final String host;
  private final int port;

  public HttpMember(String host, int port) {
    super(String.format("%s:%s", host, port));
    this.host = host;
    this.port = port;
  }

  public HttpMember(HttpMemberConfig config) {
    super(config);
    this.host = config.getHost();
    this.port = config.getPort();
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
  public String toString() {
    return String.format("%s[host=%s, port=%d]", getClass().getSimpleName(), host, port);
  }

}
