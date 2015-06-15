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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Util methods for Blobstore Cache storage.
 */
public final class Codec7BitUtil {

  private static final int ALL_BUT_LEADING_BIT_ONE_BYTE = 0x7F;

  private static final byte LEADING_BIT_ONE = (byte) 0x80;

  private static final int LONG_7BIT_BYTEARRAY_MAX_LENGTH = 10;

  private static final int MAX_CONTENT_BIT_IN_BYTE = 7;

  /**
   * Appends a long number to a byte array in its 7 bit representation.
   *
   * @param l
   *          The long number to convert and append.
   * @param byteArray
   *          The bytearray that is appended.
   * @param position
   *          The position where the long number should be appended to the byte array.
   * @return The new position within the array where data can be written.
   */
  private static int appendLongToByteArrayIn7Bit(final long l, final byte[] byteArray,
      final int position) {

    byte[] result = new byte[LONG_7BIT_BYTEARRAY_MAX_LENGTH];
    int shiftIterations = 0;
    long shiftedLong = l;
    while (shiftedLong != 0 || shiftIterations == 0) {
      byte utfByte = (byte) (((int) shiftedLong) & ALL_BUT_LEADING_BIT_ONE_BYTE);
      shiftedLong = shiftedLong >>> MAX_CONTENT_BIT_IN_BYTE;
      if (shiftIterations > 0) {
        utfByte = (byte) (utfByte | LEADING_BIT_ONE);
      }
      shiftIterations++;
      result[LONG_7BIT_BYTEARRAY_MAX_LENGTH - shiftIterations] = utfByte;
    }

    System.arraycopy(result, LONG_7BIT_BYTEARRAY_MAX_LENGTH - shiftIterations, byteArray, position,
        shiftIterations);

    return position + shiftIterations;
  }

  /**
   * Decodes 7bit encoded byte array to long array.
   *
   * @param encoded
   *          The 7bit encoded byte array.
   * @return The decoded long array.
   */
  public static long[] decode7BitToLongs(final byte[] encoded) {
    int longNum = 0;
    for (byte b : encoded) {
      if ((b & LEADING_BIT_ONE) == 0) {
        longNum++;
      }
    }
    long[] result = new long[longNum];
    int i = 0;
    for (byte b : encoded) {
      result[i] = result[i] << MAX_CONTENT_BIT_IN_BYTE;
      boolean finalByteOfLong = (b & LEADING_BIT_ONE) == 0;
      result[i] = result[i] | (b & ~LEADING_BIT_ONE);
      if (finalByteOfLong) {
        i++;
      }
    }
    return result;
  }

  /**
   * Converts one or more longs to a 7 bit byte array.
   *
   * @param longs
   *          The long numbers that are converted.
   * @return The converted byte array.
   * @throws NullPointerException
   *           if <code>longs</code> parameter is a <code>null</code> array.
   */
  public static byte[] encodeLongsTo7BitByteArray(final long... longs) {
    byte[] result = new byte[longs.length * LONG_7BIT_BYTEARRAY_MAX_LENGTH];
    int position = 0;
    for (long l : longs) {
      position = appendLongToByteArrayIn7Bit(l, result, position);
    }
    if (position == result.length) {
      return result;
    } else {
      byte[] trimmedResult = new byte[position];
      for (int i = 0; i < position; i++) {
        trimmedResult[i] = result[i];
      }
      return trimmedResult;
    }
  }

  /**
   * <p>
   * Converts an array of primitive bytes to a List. It is important to use a {@link List} instead
   * of byte[] where {@link Object#equals(Object)} and {@link Object#hashCode()} functions are used
   * for comparison (e.g.: for cache keys).
   * </p>
   *
   * <p>
   * This method returns <code>null</code> for a <code>null</code> input array.
   * </p>
   *
   * @param array
   *          a <code>byte</code> array
   * @return a <code>Byte</code> array, <code>null</code> if null array input
   */
  public static List<Byte> toUnmodifiableList(final byte[] array) {
    if (array == null) {
      return null;
    } else if (array.length == 0) {
      return Collections.emptyList();
    }
    final List<Byte> result = new ArrayList<Byte>(array.length);
    for (byte element : array) {
      result.add(element);
    }
    return Collections.unmodifiableList(result);
  }

  private Codec7BitUtil() {
  }
}
