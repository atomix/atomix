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

import org.vertx.java.core.json.JsonObject;

/**
 * State snapshot entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SnapshotEntry extends Entry {
  private Map<String, Object> data;

  public SnapshotEntry() {
    super();
  }

  public SnapshotEntry(long term, JsonObject data) {
    super(term);
    this.data = data.toMap();
  }

  @Override
  public Type type() {
    return Type.SNAPSHOT;
  }

  /**
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  public JsonObject data() {
    return new JsonObject(data);
  }

  @Override
  public String toString() {
    return String.format("Snapshot[%s]", data);
  }

}
