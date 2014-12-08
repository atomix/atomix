/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class FluentMap implements Serializable {
  private final Map<String, Object> args = new HashMap<>();

  public FluentMap put(String key, Object value) {
    args.put(key, value);
    return this;
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String key) {
    return (T) args.get(key);
  }

  public FluentMap remove(String key) {
    args.remove(key);
    return this;
  }

}
