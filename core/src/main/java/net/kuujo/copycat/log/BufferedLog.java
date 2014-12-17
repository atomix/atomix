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

import com.typesafe.config.Config;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * In-memory log implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferedLog extends AbstractLog {

  public BufferedLog(String resource) {
    super(resource);
  }

  public BufferedLog(Map<String, Object> config) {
    super(config);
  }

  public BufferedLog(Config config) {
    super(config);
  }

  public BufferedLog(String name, LogConfig config) {
    super(name, config);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected Collection<LogSegment> loadSegments() {
    return Collections.EMPTY_LIST;
  }

  @Override
  protected LogSegment createSegment(long segmentNumber) {
    return new BufferedLogSegment(this, segmentNumber);
  }

}
