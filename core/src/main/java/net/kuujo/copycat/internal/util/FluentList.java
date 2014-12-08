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
import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FluentList implements Serializable {
  private final List<Object> list = new ArrayList<>();

  public FluentList add(Object item) {
    list.add(item);
    return this;
  }

  public FluentList remove(int index) {
    list.remove(index);
    return this;
  }

  public FluentList remove(Object item) {
    list.remove(item);
    return this;
  }

  @SuppressWarnings("unchecked")
  public <T> T get(int index) {
    try {
      return (T) list.get(index);
    } catch (Exception e) {
      return null;
    }
  }

}
