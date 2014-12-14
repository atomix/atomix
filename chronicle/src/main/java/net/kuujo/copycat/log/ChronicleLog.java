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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Chronicle based Copycat log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ChronicleLog extends AbstractLog {

  public ChronicleLog(String name, LogConfig config) {
    super(name, config);
  }

  @Override
  protected Collection<LogSegment> loadSegments() {
    Map<Long, LogSegment> segments = new HashMap<>();
    base.getAbsoluteFile().getParentFile().mkdirs();
    for (File file : directory().listFiles(File::isFile)) {
      if (file.getName().startsWith(base.getName() + "-") && !segments.containsKey(Long.valueOf(file.getName().substring(0, file.getName().indexOf(".", base.getName().length()))))) {
        try {
          Long segmentNumber = Long.valueOf(file.getName().substring(0, file.getName().indexOf(".", base.getName().length())));
          // First, look for an existing history file for the log.
          File historyLogFile = new File(base.getParent(), String.format("%s-%d.history.log", base.getName(), segmentNumber));
          File historyIndexFile = new File(base.getParent(), String.format("%s-%d.history.index", base.getName(), segmentNumber));
          if (historyLogFile.exists() && historyIndexFile.exists()) {
            File logFile = new File(base.getParent(), String.format("%s-%d.log", base.getName(), segmentNumber));
            File indexFile = new File(base.getParent(), String.format("%s-%d.index", base.getName(), segmentNumber));
            Files.copy(historyLogFile.toPath(), logFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(historyIndexFile.toPath(), indexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            logFile.delete();
            indexFile.delete();
          }
          // Once we've cleaned up the history, add the segment to the log.
          if (!segments.containsKey(segmentNumber)) {
            segments.put(segmentNumber, new ChronicleLogSegment(this, segmentNumber));
          }
        } catch (IOException | NumberFormatException e) {
        }
      }
    }
    return segments.values();
  }

  @Override
  protected LogSegment createSegment(long segmentNumber) {
    return new ChronicleLogSegment(this, segmentNumber);
  }

}
