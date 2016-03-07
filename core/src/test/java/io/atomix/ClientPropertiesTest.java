/*
 * Copyright 2016 the original author or authors.
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
import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.PropertiesReader;
import io.atomix.util.ClientProperties;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.*;

/**
 * Client properties test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ClientPropertiesTest {

  /**
   * Tests default client properties.
   */
  public void testPropertyDefaults() {
    ClientProperties properties = new ClientProperties(new Properties());
    assertTrue(properties.transport() instanceof NettyTransport);
    assertTrue(properties.serializer().isWhitelistRequired());
  }

  /**
   * Tests reading properties.
   */
  public void testProperties() {
    Properties properties = new Properties();
    properties.setProperty("client.transport", "io.atomix.catalyst.transport.NettyTransport");
    properties.setProperty("client.transport.threads", "1");
    properties.setProperty("cluster.seed.1", "localhost:5000");
    properties.setProperty("cluster.seed.2", "localhost:5001");
    properties.setProperty("cluster.seed.3", "localhost:5002");
    properties.setProperty("serializer.whitelist", "false");

    ClientProperties clientProperties = new ClientProperties(properties);
    Transport transport = clientProperties.transport();
    assertTrue(transport instanceof NettyTransport);
    assertEquals(((NettyTransport) transport).properties().threads(), 1);
    assertFalse(clientProperties.serializer().isWhitelistRequired());
  }

  /**
   * Tests reading properties from a file.
   */
  public void testPropertiesFile() {
    ClientProperties clientProperties = new ClientProperties(PropertiesReader.load("core/src/test/resources/client-test.properties").properties());
    assertTrue(clientProperties.transport() instanceof NettyTransport);
    assertEquals(((NettyTransport) clientProperties.transport()).properties().threads(), 1);

    assertEquals(clientProperties.replicas().size(), 3);
    assertTrue(clientProperties.replicas().contains(new Address("localhost", 5000)));
    assertTrue(clientProperties.replicas().contains(new Address("localhost", 5001)));
    assertTrue(clientProperties.replicas().contains(new Address("localhost", 5002)));
    assertFalse(clientProperties.serializer().isWhitelistRequired());
  }

}
