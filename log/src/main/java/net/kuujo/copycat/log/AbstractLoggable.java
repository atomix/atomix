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

import net.kuujo.copycat.util.internal.Assert;

/**
 * Abstract logger.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractLoggable implements Loggable {
  /**
   * Asserts whether the log is currently open.
   */
  protected void assertIsOpen() {
    Assert.state(isOpen(), "The log is not currently open.");
  }

  /**
   * Asserts whether the log is currently closed.
   */
  protected void assertIsNotOpen() {
    Assert.state(!isOpen(), "The log is already open.");
  }

  /**
   * Asserts whether the log contains the given index.
   */
  protected void assertContainsIndex(long index) {
    Assert.index(index, containsIndex(index), "Log does not contain index %d", index);
  }
}
