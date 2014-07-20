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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * A log entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonInclude(JsonInclude.Include.ALWAYS)
@JsonAutoDetect(
  creatorVisibility=JsonAutoDetect.Visibility.NONE,
  fieldVisibility=JsonAutoDetect.Visibility.ANY,
  getterVisibility=JsonAutoDetect.Visibility.NONE,
  isGetterVisibility=JsonAutoDetect.Visibility.NONE,
  setterVisibility=JsonAutoDetect.Visibility.NONE
)
@JsonTypeInfo(
  use=JsonTypeInfo.Id.NAME,
  include=JsonTypeInfo.As.PROPERTY,
  property="type"
)
@JsonSubTypes({
  @JsonSubTypes.Type(value=NoOpEntry.class, name="no-op"),
  @JsonSubTypes.Type(value=SnapshotEntry.class, name="snapshot"),
  @JsonSubTypes.Type(value=ConfigurationEntry.class, name="configuration"),
  @JsonSubTypes.Type(value=CommandEntry.class, name="command")
})
@SuppressWarnings("serial")
public abstract class Entry implements Serializable {

  private long term;

  /**
   * Constructor.
   */
  protected Entry() {
  }

  /**
   * Constructor.
   * 
   * @param type The entry type.
   * @param term The entry term.
   */
  protected Entry(long term) {
    this.term = term;
  }

  /**
   * Returns the log entry term.
   * 
   * @return The log entry term.
   */
  public long term() {
    return term;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Entry && ((Entry) object).term() == term;
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", getClass().getSimpleName(), term);
  }

}
