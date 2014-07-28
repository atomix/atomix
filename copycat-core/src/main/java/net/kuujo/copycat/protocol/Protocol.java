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

import net.kuujo.copycat.CopyCatContext;

/**
 * CopyCat protocol.<p>
 *
 * Protocols are used by CopyCat to communicate between replicas.
 * CopyCat's protocol implementation is pluggable, meaning users can
 * use any protocol they wish to facilitate communication between nodes.
 * To register a protocol, simply create a file at
 * <code>META-INF/services/net/kuujo/copycat/protocol</code>. Name the
 * file the same name as the protocol, e.g. <code>tcp</code>. CopyCat
 * will use the protocol based on URIs used in cluster configurations.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Protocol {

  /**
   * Initializes the protocol.
   *
   * @param address The protocol address.
   * @param context The protocol context.
   */
  void init(CopyCatContext context);

  /**
   * Creates a protocol server.
   *
   * @return The protocol server.
   */
  ProtocolServer createServer();

  /**
   * Creates a protocol client.
   *
   * @return The protocol client.
   */
  ProtocolClient createClient();

}
