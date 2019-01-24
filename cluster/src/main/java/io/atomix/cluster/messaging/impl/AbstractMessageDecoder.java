/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.cluster.messaging.impl;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decoder for inbound messages.
 */
abstract class AbstractMessageDecoder extends ByteToMessageDecoder {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private static final Escape ESCAPE = new Escape();
  static final byte[] EMPTY_PAYLOAD = new byte[0];

  static class Escape extends RuntimeException {
  }

  static int readInt(ByteBuf buffer) {
    if (buffer.readableBytes() < 5) {
      return readIntSlow(buffer);
    } else {
      return readIntFast(buffer);
    }
  }

  static int readIntFast(ByteBuf buffer) {
    int b = buffer.readByte();
    int result = b & 0x7F;
    if ((b & 0x80) != 0) {
      b = buffer.readByte();
      result |= (b & 0x7F) << 7;
      if ((b & 0x80) != 0) {
        b = buffer.readByte();
        result |= (b & 0x7F) << 14;
        if ((b & 0x80) != 0) {
          b = buffer.readByte();
          result |= (b & 0x7F) << 21;
          if ((b & 0x80) != 0) {
            b = buffer.readByte();
            result |= (b & 0x7F) << 28;
          }
        }
      }
    }
    return result;
  }

  static int readIntSlow(ByteBuf buffer) {
    if (buffer.readableBytes() == 0) {
      throw ESCAPE;
    }
    buffer.markReaderIndex();
    int b = buffer.readByte();
    int result = b & 0x7F;
    if ((b & 0x80) != 0) {
      if (buffer.readableBytes() == 0) {
        buffer.resetReaderIndex();
        throw ESCAPE;
      }
      b = buffer.readByte();
      result |= (b & 0x7F) << 7;
      if ((b & 0x80) != 0) {
        if (buffer.readableBytes() == 0) {
          buffer.resetReaderIndex();
          throw ESCAPE;
        }
        b = buffer.readByte();
        result |= (b & 0x7F) << 14;
        if ((b & 0x80) != 0) {
          if (buffer.readableBytes() == 0) {
            buffer.resetReaderIndex();
            throw ESCAPE;
          }
          b = buffer.readByte();
          result |= (b & 0x7F) << 21;
          if ((b & 0x80) != 0) {
            if (buffer.readableBytes() == 0) {
              buffer.resetReaderIndex();
              throw ESCAPE;
            }
            b = buffer.readByte();
            result |= (b & 0x7F) << 28;
          }
        }
      }
    }
    return result;
  }

  static long readLong(ByteBuf buffer) {
    if (buffer.readableBytes() < 9) {
      return readLongSlow(buffer);
    } else {
      return readLongFast(buffer);
    }
  }

  static long readLongFast(ByteBuf buffer) {
    int b = buffer.readByte();
    long result = b & 0x7F;
    if ((b & 0x80) != 0) {
      b = buffer.readByte();
      result |= (b & 0x7F) << 7;
      if ((b & 0x80) != 0) {
        b = buffer.readByte();
        result |= (b & 0x7F) << 14;
        if ((b & 0x80) != 0) {
          b = buffer.readByte();
          result |= (b & 0x7F) << 21;
          if ((b & 0x80) != 0) {
            b = buffer.readByte();
            result |= (long) (b & 0x7F) << 28;
            if ((b & 0x80) != 0) {
              b = buffer.readByte();
              result |= (long) (b & 0x7F) << 35;
              if ((b & 0x80) != 0) {
                b = buffer.readByte();
                result |= (long) (b & 0x7F) << 42;
                if ((b & 0x80) != 0) {
                  b = buffer.readByte();
                  result |= (long) (b & 0x7F) << 49;
                  if ((b & 0x80) != 0) {
                    b = buffer.readByte();
                    result |= (long) b << 56;
                  }
                }
              }
            }
          }
        }
      }
    }
    return result;
  }

  static long readLongSlow(ByteBuf buffer) {
    if (buffer.readableBytes() == 0) {
      throw ESCAPE;
    }
    buffer.markReaderIndex();
    int b = buffer.readByte();
    long result = b & 0x7F;
    if ((b & 0x80) != 0) {
      if (buffer.readableBytes() == 0) {
        buffer.resetReaderIndex();
        throw ESCAPE;
      }
      b = buffer.readByte();
      result |= (b & 0x7F) << 7;
      if ((b & 0x80) != 0) {
        if (buffer.readableBytes() == 0) {
          buffer.resetReaderIndex();
          throw ESCAPE;
        }
        b = buffer.readByte();
        result |= (b & 0x7F) << 14;
        if ((b & 0x80) != 0) {
          if (buffer.readableBytes() == 0) {
            buffer.resetReaderIndex();
            throw ESCAPE;
          }
          b = buffer.readByte();
          result |= (b & 0x7F) << 21;
          if ((b & 0x80) != 0) {
            if (buffer.readableBytes() == 0) {
              buffer.resetReaderIndex();
              throw ESCAPE;
            }
            b = buffer.readByte();
            result |= (long) (b & 0x7F) << 28;
            if ((b & 0x80) != 0) {
              if (buffer.readableBytes() == 0) {
                buffer.resetReaderIndex();
                throw ESCAPE;
              }
              b = buffer.readByte();
              result |= (long) (b & 0x7F) << 35;
              if ((b & 0x80) != 0) {
                if (buffer.readableBytes() == 0) {
                  buffer.resetReaderIndex();
                  throw ESCAPE;
                }
                b = buffer.readByte();
                result |= (long) (b & 0x7F) << 42;
                if ((b & 0x80) != 0) {
                  if (buffer.readableBytes() == 0) {
                    buffer.resetReaderIndex();
                    throw ESCAPE;
                  }
                  b = buffer.readByte();
                  result |= (long) (b & 0x7F) << 49;
                  if ((b & 0x80) != 0) {
                    if (buffer.readableBytes() == 0) {
                      buffer.resetReaderIndex();
                      throw ESCAPE;
                    }
                    b = buffer.readByte();
                    result |= (long) b << 56;
                  }
                }
              }
            }
          }
        }
      }
    }
    return result;
  }

  static String readString(ByteBuf buffer, int length) {
    if (buffer.isDirect()) {
      final String result = buffer.toString(buffer.readerIndex(), length, StandardCharsets.UTF_8);
      buffer.skipBytes(length);
      return result;
    } else if (buffer.hasArray()) {
      final String result = new String(buffer.array(), buffer.arrayOffset() + buffer.readerIndex(), length, StandardCharsets.UTF_8);
      buffer.skipBytes(length);
      return result;
    } else {
      final byte[] array = new byte[length];
      buffer.readBytes(array);
      return new String(array, StandardCharsets.UTF_8);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
    try {
      log.error("Exception inside channel handling pipeline.", cause);
    } finally {
      context.close();
    }
  }
}
