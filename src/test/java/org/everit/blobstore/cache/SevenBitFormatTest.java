package org.everit.blobstore.cache;

import org.junit.Assert;
import org.junit.Test;

public class SevenBitFormatTest {

  @Test(expected = NullPointerException.class)
  public void testEncodeNull() {
    Codec7BitUtil.encodeLongsTo7BitByteArray(null);
  }

  @Test
  public void testFirstBitOneOfLastByte() {
    byte[] encoded = Codec7BitUtil.encodeLongsTo7BitByteArray(0x80);
    Assert.assertArrayEquals(new byte[] { (byte) 0x81, 0 }, encoded);
    Assert.assertArrayEquals(new long[] { 0x80 }, Codec7BitUtil.decode7BitToLongs(encoded));

  }

  public void testIntMax() {
    byte[] encoded = Codec7BitUtil.encodeLongsTo7BitByteArray(65535);
    Assert.assertArrayEquals(new byte[] { 3, (byte) 0xFF, (byte) 0xFF }, encoded);
    Assert.assertArrayEquals(new long[] { 65535 }, Codec7BitUtil.decode7BitToLongs(encoded));
  }

  @Test
  public void testMultipleLongs() {
    byte[] encoded = Codec7BitUtil.encodeLongsTo7BitByteArray(0, Long.MIN_VALUE, 1, 0,
        Long.MAX_VALUE);

    Assert.assertArrayEquals(new long[] { 0, Long.MIN_VALUE, 1, 0, Long.MAX_VALUE },
        Codec7BitUtil.decode7BitToLongs(encoded));
  }

  @Test
  public void testOne() {
    byte[] encoded = Codec7BitUtil.encodeLongsTo7BitByteArray(1);
    Assert.assertArrayEquals(new byte[] { 1 }, encoded);
    Assert.assertArrayEquals(new long[] { 1 }, Codec7BitUtil.decode7BitToLongs(encoded));
  }

  @Test
  public void testZero() {
    byte[] encoded = Codec7BitUtil.encodeLongsTo7BitByteArray(0);
    Assert.assertArrayEquals(new byte[] { 0 }, encoded);
    Assert.assertArrayEquals(new long[] { 0 }, Codec7BitUtil.decode7BitToLongs(encoded));

  }
}
