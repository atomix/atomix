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

import io.atomix.catalyst.transport.NettyTransport;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.PropertiesReader;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
  }

  /**
   * Tests reading properties.
   */
  public void testProperties() {
    Properties properties = new Properties();
    properties.setProperty(ClientProperties.TRANSPORT, "io.atomix.catalyst.transport.NettyTransport");
    properties.setProperty("transport.threads", "1");

    ClientProperties clientProperties = new ClientProperties(properties);
    Transport transport = clientProperties.transport();
    assertTrue(transport instanceof NettyTransport);
    assertEquals(((NettyTransport) transport).properties().threads(), 1);
  }

  /**
   * Tests reading properties from a file.
   */
  public void testPropertiesFile() {
    ClientProperties clientProperties = new ClientProperties(PropertiesReader.load("client-test.properties").properties());
    assertTrue(clientProperties.transport() instanceof NettyTransport);
    assertEquals(((NettyTransport) clientProperties.transport()).properties().threads(), 1);
  }

}
