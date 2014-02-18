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
package net.kuujo.copycat.state.impl;

import java.io.File;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.file.FileSystem;
import org.vertx.java.core.impl.DefaultFutureResult;
import org.vertx.java.core.json.DecodeException;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

/**
 * A snapshot persistor.
 *
 * @author Jordan Halterman
 */
public final class SnapshotPersistor {
  private String filename;
  private String directory;
  private final FileSystem fileSystem;

  public SnapshotPersistor(String filename, FileSystem fileSystem) {
    this.filename = filename;
    File directory = new File(filename).getParentFile();
    if (directory != null) {
      this.directory = directory.getName();
    }
    else {
      this.directory = null;
    }
    this.fileSystem = fileSystem;
  }

  /**
   * Returns the snapshot file name.
   *
   * @return The snapshot file name.
   */
  public String getSnapshotFile() {
    return filename;
  }

  /**
   * Sets the snapshot file name.
   *
   * @param filename The snapshot file name.
   * @return The snapshot persistor.
   */
  public SnapshotPersistor setSnapshotFile(String filename) {
    this.filename = filename;
    File directory = new File(filename).getParentFile();
    if (directory != null) {
      this.directory = directory.getName();
    }
    else {
      this.directory = null;
    }
    return this;
  }

  /**
   * Stores a snapshot.
   *
   * @param snapshot The snapshot to store.
   */
  public void storeSnapshot(final JsonElement snapshot, final Handler<AsyncResult<Void>> doneHandler) {
    if (directory != null) {
      fileSystem.exists(directory, new Handler<AsyncResult<Boolean>>() {
        @Override
        public void handle(AsyncResult<Boolean> result) {
          if (result.failed()) {
            new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
          }
          else if (!result.result()) {
            fileSystem.mkdir(directory, new Handler<AsyncResult<Void>>() {
              @Override
              public void handle(AsyncResult<Void> result) {
                if (result.failed()) {
                  new DefaultFutureResult<Void>(result.cause()).setHandler(doneHandler);
                }
                else {
                  fileSystem.writeFile(filename, new Buffer(snapshot.isObject() ? snapshot.asObject().encode() : snapshot.asArray().encode()), doneHandler);
                }
              }
            });
          }
          else {
            fileSystem.writeFile(filename, new Buffer(snapshot.isObject() ? snapshot.asObject().encode() : snapshot.asArray().encode()), doneHandler);
          }
        }
      });
    }
    else {
      fileSystem.writeFile(filename, new Buffer(snapshot.isObject() ? snapshot.asObject().encode() : snapshot.asArray().encode()), doneHandler);
    }
  }

  /**
   * Loads a snapshot.
   *
   * @return The loaded snapshot.
   */
  public void loadSnapshot(final Handler<AsyncResult<JsonElement>> doneHandler) {
    fileSystem.exists(filename, new Handler<AsyncResult<Boolean>>() {
      @Override
      public void handle(AsyncResult<Boolean> result) {
        if (result.failed()) {
          new DefaultFutureResult<JsonElement>(result.cause()).setHandler(doneHandler);
        }
        else if (!result.result()) {
          new DefaultFutureResult<JsonElement>(new JsonObject()).setHandler(doneHandler);
        }
        else {
          fileSystem.readFile(filename, new Handler<AsyncResult<Buffer>>() {
            @Override
            public void handle(AsyncResult<Buffer> result) {
              if (result.failed()) {
                new DefaultFutureResult<JsonElement>(result.cause()).setHandler(doneHandler);
              }
              else {
                JsonElement snapshot;
                try {
                  snapshot = new JsonObject(result.result().toString());
                }
                catch (DecodeException e) {
                  snapshot = new JsonArray(result.result().toString());
                }
                new DefaultFutureResult<JsonElement>(snapshot).setHandler(doneHandler);
              }
            }
          });
        }
      }
    });
  }

}
