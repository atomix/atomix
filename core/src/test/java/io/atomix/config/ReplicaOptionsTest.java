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
package io.atomix.config;

import static org.testng.Assert.assertEquals;

import java.util.Properties;

import org.testng.annotations.Test;

import io.atomix.catalyst.util.PropertiesReader;

/**
 * Replica options test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ReplicaOptionsTest {

  /**
   * Tests default server properties.
   */
  public void testPropertyDefaults() {
    ReplicaOptions options = new ReplicaOptions(new Properties());
    assertEquals(options.quorumHint(), -1);
    assertEquals(options.backupCount(), 0);
  }

  /**
   * Tests reading properties.
   */
  public void testProperties() {
    Properties properties = new Properties();
    properties.setProperty("cluster.quorumHint", "3");
    properties.setProperty("cluster.backupCount", "1");

    ReplicaOptions options = new ReplicaOptions(properties);

    assertEquals(options.quorumHint(), 3);
    assertEquals(options.backupCount(), 1);
  }

  /**
   * Tests reading properties from a file.
   */
  public void testPropertiesFile() {
    ReplicaOptions options = new ReplicaOptions(PropertiesReader.loadFromClasspath("replica-test.properties").properties());

    assertEquals(options.quorumHint(), 3);
    assertEquals(options.backupCount(), 1);
  }

}
