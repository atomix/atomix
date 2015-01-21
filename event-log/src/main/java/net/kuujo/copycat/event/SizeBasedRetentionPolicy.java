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

import net.kuujo.copycat.AbstractConfigurable;
import net.kuujo.copycat.log.LogSegment;

/**
 * Size based log compaction strategy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SizeBasedRetentionPolicy extends AbstractConfigurable implements RetentionPolicy {
  private long size;

  public SizeBasedRetentionPolicy() {
  }

  public SizeBasedRetentionPolicy(long size) {
    this.size = size;
  }

  private SizeBasedRetentionPolicy(SizeBasedRetentionPolicy policy) {
    super(policy);
  }

  @Override
  public SizeBasedRetentionPolicy copy() {
    return new SizeBasedRetentionPolicy(this);
  }

  /**
   * Sets the retention size.
   *
   * @param size The retention size.
   */
  public void setSize(long size) {
    this.size = size;
  }

  /**
   * Returns the retention size.
   *
   * @return The retention size.
   */
  public long getSize() {
    return size;
  }

  /**
   * Sets the retention size, returning the retention policy for method chaining.
   *
   * @param size The retention size.
   * @return The retention policy.
   */
  public SizeBasedRetentionPolicy withSize(long size) {
    this.size = size;
    return this;
  }

  @Override
  public boolean retain(LogSegment segment) {
    return segment.log().size() < size;
  }

}
