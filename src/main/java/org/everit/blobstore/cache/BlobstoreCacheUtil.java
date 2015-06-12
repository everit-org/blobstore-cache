/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.blobstore.cache;

/**
 * Util methods for Blobstore Cache storage.
 */
public final class BlobstoreCacheUtil {

  private static final int ALL_BUT_LEADING_BIT_ONE_BYTE = 0x7F;

  private static final byte LEADING_BIT_ONE = (byte) 0x80;

  private static final int LONG_7BIT_BYTEARRAY_MAX_LENGTH = 10;

  private static final int MAX_CONTENT_BIT_IN_BYTE = 7;

  public static int appendLongToByteArrayIn7Bit(final long l, final Byte[] byteArray,
      final int position) {

    Byte[] result = new Byte[LONG_7BIT_BYTEARRAY_MAX_LENGTH];
    int shiftIterations = 0;
    long shiftedLong = l;
    while (shiftedLong != 0 || shiftIterations == 0) {
      byte utfByte = (byte) (((int) shiftedLong) & ALL_BUT_LEADING_BIT_ONE_BYTE);
      shiftedLong = shiftedLong >> MAX_CONTENT_BIT_IN_BYTE;
      shiftIterations++;
      if (shiftedLong != 0) {
        utfByte = (byte) (utfByte | LEADING_BIT_ONE);
      }
      result[LONG_7BIT_BYTEARRAY_MAX_LENGTH - shiftIterations] = utfByte;
    }
    if ((result[LONG_7BIT_BYTEARRAY_MAX_LENGTH - shiftIterations] & LEADING_BIT_ONE) != 0) {
      result[LONG_7BIT_BYTEARRAY_MAX_LENGTH - ++shiftIterations] = LEADING_BIT_ONE;
    }

    System.arraycopy(result, LONG_7BIT_BYTEARRAY_MAX_LENGTH - shiftIterations, byteArray, position,
        shiftIterations);

    return position + shiftIterations;
  }

  public static Byte[] encodeLongsTo7BitByteArray(final long... longs) {
    Byte[] result = new Byte[longs.length * LONG_7BIT_BYTEARRAY_MAX_LENGTH];
    int position = 0;
    for (long l : longs) {
      position = appendLongToByteArrayIn7Bit(l, result, position);
    }
    if (position == result.length) {
      return result;
    } else {
      Byte[] trimmedResult = new Byte[position];
      for (int i = 0; i < position; i++) {
        trimmedResult[i] = result[i];
      }
      return trimmedResult;
    }
  }

  private BlobstoreCacheUtil() {
  }
}
