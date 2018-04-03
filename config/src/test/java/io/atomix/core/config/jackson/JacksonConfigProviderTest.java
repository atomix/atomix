/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.config.jackson;

import io.atomix.core.AtomixConfig;
import io.atomix.core.config.ConfigProvider;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Jackson configuration provider test.
 */
public class JacksonConfigProviderTest {
  @Test
  public void testJson() throws Exception {
    ConfigProvider provider = new JacksonConfigProvider();
    File file = new File(getClass().getClassLoader().getResource("config.json").getFile());
    assertTrue(provider.isConfigFile(file));
    AtomixConfig config = provider.load(file, AtomixConfig.class);
    assertEquals("test", config.getClusterConfig().getName());
    assertEquals(1, config.getPrimitives().get("foo").getSerializerConfig().getTypes().size());
  }

  @Test
  public void testYaml() throws Exception {
    ConfigProvider provider = new JacksonConfigProvider();
    File file = new File(getClass().getClassLoader().getResource("config.yaml").getFile());
    assertTrue(provider.isConfigFile(file));
    AtomixConfig config = provider.load(file, AtomixConfig.class);
    assertEquals("test", config.getClusterConfig().getName());
    assertEquals(1, config.getPrimitives().get("foo").getSerializerConfig().getTypes().size());
  }
}
