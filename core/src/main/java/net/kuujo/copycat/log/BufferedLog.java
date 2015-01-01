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
 * Buffered log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferedLog extends Log {

  public BufferedLog() {
    super();
  }

  public BufferedLog(Map<String, Object> config) {
    super(config);
  }

  private BufferedLog(BufferedLog log) {
    super(log);
  }

  @Override
  public BufferedLog copy() {
    return new BufferedLog(this);
  }

  @Override
  public BufferedLog withSegmentSize(int segmentSize) {
    setSegmentSize(segmentSize);
    return this;
  }

  @Override
  public BufferedLog withSegmentInterval(long segmentInterval) {
    setSegmentInterval(segmentInterval);
    return this;
  }

  @Override
  public BufferedLog withFlushOnWrite(boolean flushOnWrite) {
    setFlushOnWrite(flushOnWrite);
    return this;
  }

  @Override
  public BufferedLog withFlushInterval(long flushInterval) {
    setFlushInterval(flushInterval);
    return this;
  }

  @Override
  public BufferedLog withRetentionPolicy(RetentionPolicy retentionPolicy) {
    setRetentionPolicy(retentionPolicy);
    return this;
  }

  @Override
  public LogManager getLogManager(String name) {
    return new BufferedLogManager(this);
  }

}
