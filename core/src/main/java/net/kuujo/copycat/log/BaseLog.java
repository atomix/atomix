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

import net.kuujo.copycat.internal.util.Assert;
import net.kuujo.copycat.util.serializer.Serializer;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract base log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class BaseLog implements Log {
  protected final LogConfig config;
  protected final Serializer serializer = Serializer.serializer();

  protected BaseLog(LogConfig config) {
    this.config = config;
  }

  @Override
  public List<Long> appendEntries(Entry... entries) {
    assertIsOpen();
    return Arrays.stream(entries).map(entry -> appendEntry(entry)).collect(Collectors.toList());
  }

  @Override
  public List<Long> appendEntries(List<Entry> entries) {
    Assert.isNotNull(entries, "entries");
    assertIsOpen();
    return entries.stream().map(entry -> appendEntry(entry)).collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return String.format("%s[size=%d]", getClass().getSimpleName(), size());
  }

  protected void assertIsOpen() {
    Assert.state(isOpen(), "The log is not currently open.");
  }

  protected void assertIsNotOpen() {
    Assert.state(!isOpen(), "The log is already open.");
  }

}
