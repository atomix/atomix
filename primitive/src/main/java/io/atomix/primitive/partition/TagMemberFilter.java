/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.partition;

import io.atomix.cluster.Node;

/**
 * Tag-based member filter.
 */
public class TagMemberFilter implements TypedMemberFilter {

  /**
   * Returns a new tag-based member filter.
   *
   * @param tag the tag on which to filter
   * @return the tag based member filter
   */
  public TagMemberFilter tag(String tag) {
    return new TagMemberFilter(tag);
  }

  private static final String TYPE = "tag";

  private String tag;

  public TagMemberFilter() {
    this(null);
  }

  public TagMemberFilter(String tag) {
    this.tag = tag;
  }

  @Override
  public String type() {
    return TYPE;
  }

  /**
   * Returns the filter tag.
   *
   * @return the filter tag
   */
  public String tag() {
    return tag;
  }

  @Override
  public boolean isMember(Node node) {
    return node.tags().contains(tag);
  }
}
