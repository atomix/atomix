/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.cluster.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.protocol.ProtocolInstance;
import net.kuujo.copycat.protocol.impl.DefaultProtocolFactory;

/**
 * Default cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultMember implements Member {
  private final String uri;
  private final ProtocolInstance protocol;

  public DefaultMember(String uri, CopyCatContext context) {
    this.uri = uri;
    this.protocol = new DefaultProtocolFactory(context).createProtocol(uri);
  }

  @Override
  public String name() {
    return protocol.name();
  }

  @Override
  public String uri() {
    return uri;
  }

  @Override
  public ProtocolInstance protocol() {
    return protocol;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Member && ((Member) object).uri().equals(uri);
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + uri.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return uri;
  }

}
