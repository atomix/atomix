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

import java.util.Map;

/**
 * Copycat log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Log extends LogConfig {

  protected Log() {
    super();
  }

  protected Log(Map<String, Object> config) {
    super(config);
  }

  protected Log(Log log) {
    super(log);
  }

  @Override
  public abstract Log copy();

  @Override
  public Log withSegmentSize(int segmentSize) {
    setSegmentSize(segmentSize);
    return this;
  }

  @Override
  public Log withSegmentInterval(long segmentInterval) {
    setSegmentInterval(segmentInterval);
    return this;
  }

  @Override
  public Log withFlushOnWrite(boolean flushOnWrite) {
    setFlushOnWrite(flushOnWrite);
    return this;
  }

  @Override
  public Log withFlushInterval(long flushInterval) {
    setFlushInterval(flushInterval);
    return this;
  }

  /**
   * Gets a log manager for the given resource.
   *
   * @param name The resource name.
   * @return The resource log manager.
   */
  public abstract LogManager getLogManager(String name);

}
