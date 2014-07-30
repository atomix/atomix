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
package net.kuujo.copycat.log.impl;

import net.kuujo.copycat.log.Entry;

/**
 * No-op (empty) log entry.<p>
 *
 * This type of entry is used at the leader's discretion to commit
 * an empty entry to its log. The no-op entry can be used to log
 * and replicate the leader's current term and index in order to
 * ensure that other replicas are up-to-date.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NoOpEntry extends Entry {
  private static final long serialVersionUID = 5240217873800235626L;

  public NoOpEntry() {
    super();
  }

  public NoOpEntry(long term) {
    super(term);
  }

}
