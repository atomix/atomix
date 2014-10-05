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

import net.kuujo.copycat.spi.protocol.ProtocolClient;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RemoteMember<M extends MemberConfig> extends Member<M> {
  private final ProtocolClient client;

  public RemoteMember(ProtocolClient client, M config) {
    super(config);
    this.client = client;
  }

  /**
   * Returns the remote member client.
   *
   * @return The remote member client.
   */
  public ProtocolClient client() {
    return client;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof RemoteMember && ((Member<?>) object).config().equals(config());
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + config().hashCode();
    hashCode = 37 * hashCode + client.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("RemoteMember[config=%s]", config());
  }

}
