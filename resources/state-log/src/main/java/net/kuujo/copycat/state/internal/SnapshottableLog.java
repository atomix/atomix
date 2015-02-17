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
package net.kuujo.copycat.state.internal;

import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.log.LogManager;
import net.kuujo.copycat.util.Configurable;

import java.util.Map;

/**
 * Log decorator that handles snapshots.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SnapshottableLog extends Log {

  public SnapshottableLog() {
    super();
  }

  public SnapshottableLog(Map<String, Object> config) {
    super(config);
  }

  public SnapshottableLog(Log log) {
    super();
    config.put("log", log.toMap());
  }

  @Override
  public SnapshottableLog copy() {
    return new SnapshottableLog(toMap());
  }

  @Override
  public LogManager getLogManager(String name) {
    Log log = Configurable.load((Map<String, Object>) config.get("log"));
    LogManager logManager = log.getLogManager(name);
    return new SnapshottableLogManager(logManager, log.copy()
      .getLogManager(String.format("%s.snapshot", name)));
  }

}
