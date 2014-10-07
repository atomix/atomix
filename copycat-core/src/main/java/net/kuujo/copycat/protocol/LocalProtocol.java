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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.spi.protocol.CopycatProtocol;
import net.kuujo.copycat.spi.protocol.ProtocolClient;
import net.kuujo.copycat.spi.protocol.ProtocolServer;

import java.util.HashMap;
import java.util.Map;

/**
 * Local protocol implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocol implements CopycatProtocol<Member> {
  private final Map<String, LocalProtocolServer> registry = new HashMap<>(10);

  public LocalProtocol() {
  }

  @Override
  public ProtocolServer createServer(Member member) {
    return new LocalProtocolServer(member.id(), registry);
  }

  @Override
  public ProtocolClient createClient(Member member) {
    return new LocalProtocolClient(member.id(), registry);
  }

  @Override
  public String toString() {
    return "LocalProtocol";
  }

}
