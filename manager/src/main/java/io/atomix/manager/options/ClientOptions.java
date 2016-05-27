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
 * limitations under the License
 */
package io.atomix.manager.options;

import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.QualifiedProperties;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Client options.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ClientOptions extends AtomixOptions {
  public static final String TRANSPORT = "client.transport";

  private static final String DEFAULT_TRANSPORT = "io.atomix.catalyst.transport.netty.NettyTransport";

  public ClientOptions(Properties properties) {
    super(properties);
  }

  /**
   * Returns the replica transport.
   *
   * @return The replica transport.
   */
  public Transport transport() {
    String transportClass = reader.getString(TRANSPORT, DEFAULT_TRANSPORT);
    try {
      return (Transport) Class.forName(transportClass).getConstructor(Properties.class).newInstance(new QualifiedProperties(reader.properties(), TRANSPORT));
    } catch (ClassNotFoundException e) {
      throw new ConfigurationException("unknown transport class: " + transportClass, e);
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
      throw new ConfigurationException("failed to instantiate transport", e);
    }
  }

}
