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

import net.kuujo.copycat.log.impl.CommandEntry;
import net.kuujo.copycat.log.impl.ConfigurationEntry;
import net.kuujo.copycat.log.impl.NoOpEntry;
import net.kuujo.copycat.log.impl.SnapshotChunkEntry;
import net.kuujo.copycat.log.impl.SnapshotEndEntry;
import net.kuujo.copycat.log.impl.SnapshotStartEntry;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Log entry.<p>
 *
 * Log entries represent events that are appended to the local
 * log. CopyCat uses logged events to manage cluster configuration
 * and replicate state information across the cluster. While CopyCat
 * uses several types of entries to communicate different events,
 * each entry has an associated log index and term.
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
  @JsonSubTypes.Type(value=ConfigurationEntry.class, name="configuration"),
  @JsonSubTypes.Type(value=CommandEntry.class, name="command"),
  @JsonSubTypes.Type(value=SnapshotStartEntry.class, name="snapshot-start"),
  @JsonSubTypes.Type(value=SnapshotChunkEntry.class, name="snapshot-chunk"),
  @JsonSubTypes.Type(value=SnapshotEndEntry.class, name="snapshot-end")
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
    return getClass().isAssignableFrom(object.getClass()) && ((Entry) object).term() == term;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%s]", getClass().getSimpleName(), term);
  }

}
