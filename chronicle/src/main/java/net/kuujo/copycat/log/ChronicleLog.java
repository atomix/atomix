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

import net.kuujo.copycat.Config;
import net.openhft.chronicle.ChronicleConfig;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.chronicle.IndexedChronicle;

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
  ChronicleConfig chronicleConfig;

  @Override
  public void configure(Config baseConfig) {
    ChronicleLogConfig config = new ChronicleLogConfig(baseConfig);
    chronicleConfig = ChronicleConfig.DEFAULT
      .indexFileCapacity(config.getIndexFileCapacity())
      .indexFileExcerpts(config.getIndexFileExcerpts())
      .dataBlockSize(config.getDataBlockSize())
      .messageCapacity(config.getMessageCapacity());
    chronicleConfig.minimiseFootprint(config.isMinimiseFootprint());
  }

  @Override
  protected Collection<LogSegment> loadSegments() {
    Map<Long, LogSegment> segments = new HashMap<>();
    base.getAbsoluteFile().getParentFile().mkdirs();
    for (File file : directory().listFiles(File::isFile)) {
      if (file.getName().startsWith(base.getName() + "-")
        && !segments.containsKey(Long.valueOf(file.getName().substring(0,
          file.getName().indexOf(".", base.getName().length()))))) {
        try {
          long id = Long.valueOf(file.getName().substring(0, file.getName().indexOf(".", base.getName().length()))).longValue();
          // First, look for an existing history file for the log.
          File historyLogFile = new File(base.getParent(), String.format("%s-%d.history.log", base.getName(), id));
          File historyIndexFile = new File(base.getParent(), String.format("%s-%d.history.index", base.getName(), id));
          if (historyLogFile.exists() && historyIndexFile.exists()) {
            File logFile = new File(base.getParent(), String.format("%s-%d.log", base.getName(), id));
            File indexFile = new File(base.getParent(), String.format("%s-%d.index", base.getName(), id));
            Files.copy(historyLogFile.toPath(), logFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            Files.copy(historyIndexFile.toPath(), indexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            logFile.delete();
            indexFile.delete();
          }

          long firstIndex = firstEntryIndex(file);

          // Once we've cleaned up the history, add the segment to the log.
          if (!segments.containsKey(id)) {
            segments.put(id, new ChronicleLogSegment(this, id, firstIndex));
          }
        } catch (IOException | NumberFormatException e) {
          throw new LogException(e);
        }
      }
    }
    return segments.values();
  }

  @Override
  protected LogSegment createSegment(long id, long firstIndex) {
    return new ChronicleLogSegment(this, id, firstIndex);
  }

  long firstEntryIndex(File file) throws IOException {
    try (IndexedChronicle chronicle = new IndexedChronicle(file.getAbsolutePath())) {
      ExcerptTailer tailer = chronicle.createTailer();
      try (ExcerptTailer t = tailer.toStart()) {
        return t.readLong();
      }
    }
  }
}
