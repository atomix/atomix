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

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.kuujo.copycat.log.Entry;

/**
 * Abstract file-based log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractFileLog extends AbstractLog {
  private static final SimpleDateFormat fileNameFormat = new SimpleDateFormat("yyyyMMddhhmmssSSS");
  private final File baseFile;

  protected AbstractFileLog(File baseFile, Class<? extends Entry> entryType) {
    super(entryType);
    this.baseFile = baseFile;
  }

  /**
   * Finds the most recent long file.
   */
  protected final File findLogFile() {
    baseFile.getAbsoluteFile().getParentFile().mkdirs();
    File logFile = null;
    long logTime = 0;
    for (File file : baseFile.getAbsoluteFile().getParentFile().listFiles(file -> file.isFile())) {
      if (file.getName().substring(0, file.getName().indexOf('.')).equals(baseFile.getName())) {
        try {
          long fileTime = fileNameFormat.parse(file.getName().substring(file.getName().indexOf('.') + 1, file.getName().indexOf('.', file.getName().indexOf('.') + 1))).getTime();
          if (fileTime > logTime) {
            logFile = new File(file.getAbsoluteFile().getParentFile().getAbsolutePath(), file.getName().substring(0, file.getName().indexOf('.', file.getName().indexOf('.') + 1)));
            logTime  = fileTime;
          }
        } catch (ParseException e) {
        }
      }
    }

    if (logFile == null) {
      logFile = createLogFile();
    }
    return logFile;
  }

  /**
   * Creates a new log file.
   */
  protected final File createLogFile() {
    return new File(baseFile.getAbsoluteFile().getParentFile().getAbsolutePath(), String.format("%s.%s", baseFile.getName(), fileNameFormat.format(new Date())));
  }

  /**
   * Deletes a log file.
   */
  protected final void deleteLogFile(File logFile) {
    for (File file : baseFile.getAbsoluteFile().getParentFile().listFiles(file -> file.isFile())) {
      if (file.getName().startsWith(logFile.getName())) {
        file.delete();
      }
    }
  }

}
