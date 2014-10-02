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
package net.kuujo.copycat;

import java.io.IOException;
import java.util.UUID;

import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.impl.MemoryMappedLog;

import org.junit.After;
import org.junit.Before;

/**
 * File log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PersistentLogTest extends LogTest {
  private Log log;

  @Before
  public void beforeClass() {
    log = new MemoryMappedLog(UUID.randomUUID().toString());
  }

  @Override
  protected Log createLog() {
    return log;
  }

  @After
  public void afterClass() throws IOException {
    log.delete();
    log = null;
  }

}
