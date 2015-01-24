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
package net.kuujo.copycat.event;

import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.log.LogSegment;

/**
 * Log retention policy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RetentionPolicy extends Configurable {

  /**
   * Returns a boolean value indicating whether the given log segment should be retained.
   *
   * @param segment The segment to check.
   * @return Indicates whether the given log segment should be retained.
   */
  boolean retain(LogSegment segment);

}
