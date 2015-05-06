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
package net.kuujo.copycat.protocol;

import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.util.ExecutionContext;

/**
 * Copycat protocol.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Protocol {

  /**
   * Creates a new protocol instance.
   *
   * @param name The protocol name.
   * @param cluster The protocol cluster.
   * @param context The protocol execution context.
   * @return The protocol instance.
   */
  ProtocolInstance createInstance(String name, Cluster cluster, ExecutionContext context);

  /**
   * Protocol builder.
   */
  static interface Builder extends net.kuujo.copycat.Builder<Protocol> {
  }

}
