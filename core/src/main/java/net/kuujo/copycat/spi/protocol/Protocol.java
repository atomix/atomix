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
package net.kuujo.copycat.spi.protocol;

import java.net.URI;

/**
 * Copycat protocol.
 * <p>
 *
 * Protocols are used by Copycat to communicate between replicas. Copycat's protocol implementation
 * is pluggable, meaning users can use any protocol they wish to facilitate communication between
 * nodes.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Protocol {
  /**
   * Creates a protocol server.
   *
   * @param member The member configuration.
   * @return The protocol server.
   */
  ProtocolServer createServer(URI endpoint);

  /**
   * Creates a protocol client.
   *
   * @param member The member configuration.
   * @return The protocol client.
   */
  ProtocolClient createClient(URI endpoint);
}
