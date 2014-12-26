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
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * File log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class FileLog extends AbstractLog {

  @Override
  protected Collection<LogSegment> loadSegments() {
    Map<Long, LogSegment> segments = new HashMap<>();
    base.getAbsoluteFile().getParentFile().mkdirs();
    for (File file : directory().listFiles(File::isFile)) {
      if (file.getName().startsWith(base.getName() + "-") && file.getName().endsWith(".metadata")) {
        try {
          long id = Long.valueOf(file.getName().substring(file.getName().lastIndexOf('-')), file.getName().lastIndexOf('.')).longValue();
          if (!segments.containsKey(id)) {
            // First, look for an existing history file for the log. If history files exist for this segment then that
            // indicates that a failure occurred during log compaction. Recover the previous log.
            File historyLogFile = new File(base.getParent(), String.format("%s-%d.log.history", base.getName(), id));
            File historyIndexFile = new File(base.getParent(), String.format("%s-%d.index.history", base.getName(), id));
            File historyMetadataFile = new File(base.getParent(), String.format("%s-%d.metadata.history", base.getName(), id));
            if (historyLogFile.exists() && historyIndexFile.exists() && historyMetadataFile.exists()) {
              // Restore the log by moving historical files back to permanent log files.
              File logFile = new File(base.getParent(), String.format("%s-%d.log", base.getName(), id));
              File indexFile = new File(base.getParent(), String.format("%s-%d.index", base.getName(), id));
              File metadataFile = new File(base.getParent(), String.format("%s-%d.metadata", base.getName(), id));

              // Copy the files instead of moving them in case another failure occurs.
              Files.copy(historyLogFile.toPath(), logFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
              Files.copy(historyIndexFile.toPath(), indexFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
              Files.copy(historyMetadataFile.toPath(), metadataFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

              // Once the history has been restored, delete historical files.
              historyLogFile.delete();
              historyIndexFile.delete();
              historyMetadataFile.delete();
            }

            // Open the metadata file, determine the segment's first index, and create a log segment.
            try (RandomAccessFile metaFile = new RandomAccessFile(file, "r")) {
              long firstIndex = metaFile.readLong();
              segments.put(id, new FileLogSegment(this, id, firstIndex));
            }
          }
        } catch (IOException | NumberFormatException e) {
          throw new LogException(e);
        }
      }
    }
    return segments.values();
  }

  @Override
  protected LogSegment createSegment(long segmentId, long firstIndex) {
    return new FileLogSegment(this, segmentId, firstIndex);
  }

}
