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
package net.kuujo.copycat.log;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Log configuration test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class LogConfigTest {

  /**
   * Tests loading the default log configuration.
   */
  public void testDefaultConfig() {
    LogConfig config = new LogConfig();
    assertEquals(config.getDirectory().getName(), "copycat");
    assertEquals(config.getSegmentSize(), 1000);
    assertEquals(config.getSegmentInterval(), 60000);
  }

}
