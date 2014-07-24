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
import net.kuujo.copycat.protocol.ProtocolFactory;
import net.kuujo.copycat.protocol.ProtocolInstance;
import net.kuujo.copycat.protocol.impl.DefaultProtocolFactory;
import net.kuujo.copycat.protocol.impl.DefaultProtocolInstance;

/**
 * Default cluster member.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultMember implements Member {
  private final String address;
  private final ProtocolInstance protocol;

  public DefaultMember(String address, CopyCatContext context) {
    this.address = address;
    ProtocolFactory factory = new DefaultProtocolFactory(context);
    this.protocol = new DefaultProtocolInstance(factory.createProtocol(address));
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public ProtocolInstance protocol() {
    return protocol;
  }

}
