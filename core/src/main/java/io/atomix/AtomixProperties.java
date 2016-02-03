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
package io.atomix;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.PropertiesReader;
import io.atomix.catalyst.util.QualifiedProperties;

import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Properties;

/**
 * Base class for Atomix properties.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AtomixProperties {
  public static final String TRANSPORT = "transport";
  public static final String REPLICA = "replica";

  private static final String DEFAULT_TRANSPORT = "io.atomix.catalyst.transport.NettyTransport";

  protected final PropertiesReader reader;

  protected AtomixProperties(Properties properties) {
    this.reader = new PropertiesReader(properties);
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

  /**
   * Returns the collection of replicas.
   *
   * @return The collection of replicas.
   */
  public Collection<Address> replicas() {
    return reader.getCollection(REPLICA, p -> parseAddress(reader.getString(p)));
  }

  /**
   * Parses an address string.
   *
   * @param address The address string.
   * @return The address.
   */
  protected Address parseAddress(String address) {
    String[] split = address.split(":");
    if (split.length != 2) {
      throw new ConfigurationException("malformed address: " + address);
    }

    try {
      return new Address(split[0], Integer.valueOf(split[1]));
    } catch (NumberFormatException e) {
      throw new ConfigurationException("invalid port number: " + split[1]);
    }
  }

}
