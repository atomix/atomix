/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.config;

import java.util.Properties;

import io.atomix.Quorum;
import io.atomix.manager.options.ServerOptions;

/**
 * Replica options.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ReplicaOptions extends ServerOptions {
  public static final String QUORUM_HINT = "cluster.quorumHint";
  public static final String BACKUP_COUNT = "cluster.backupCount";

  private static final int DEFAULT_QUORUM_HINT = -1;
  private static final int DEFAULT_BACKUP_COUNT = 0;

  public ReplicaOptions(Properties properties) {
    super(properties);
  }

  /**
   * Returns the quorum hint.
   *
   * @return The quorum hint.
   */
  public int quorumHint() {
    String quorumHint = reader.getString(QUORUM_HINT, String.valueOf(DEFAULT_QUORUM_HINT));
    try {
      return Integer.valueOf(quorumHint);
    } catch (NumberFormatException e) {
      return Quorum.valueOf(quorumHint.trim().toUpperCase()).size();
    }
  }

  /**
   * Returns the backup count.
   *
   * @return The backup count.
   */
  public int backupCount() {
    return reader.getInteger(BACKUP_COUNT, DEFAULT_BACKUP_COUNT);
  }

}
