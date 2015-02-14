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
package net.kuujo.copycat.event.retention;

import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.log.LogSegment;

import java.util.Map;

/**
 * Retention policy that does not retain any logs.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ZeroRetentionPolicy extends AbstractConfigurable implements RetentionPolicy {

  public ZeroRetentionPolicy() {
    super();
  }

  public ZeroRetentionPolicy(Map<String, Object> config) {
    super(config);
  }

  private ZeroRetentionPolicy(ZeroRetentionPolicy policy) {
    super(policy);
  }

  @Override
  public ZeroRetentionPolicy copy() {
    return new ZeroRetentionPolicy(this);
  }

  @Override
  public boolean retain(LogSegment segment) {
    return false;
  }

}
