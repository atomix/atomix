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

import java.util.UUID;

import org.testng.annotations.Test;

/**
 * File log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class ChronicleLogTest extends AbstractLogTest {
  @Override
  protected Log createLog() {
    return new ChronicleLog(String.format("target/test-logs/%s", UUID.randomUUID()));
  }

  @Override
  protected void deleteLog() throws Throwable {
    log.delete();
  }
}
