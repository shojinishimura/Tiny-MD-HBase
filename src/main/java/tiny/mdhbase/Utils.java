/*
 * Copyright 2012 Shoji Nishimura
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package tiny.mdhbase;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author shoji
 * 
 */
public class Utils {
  private Utils() {

  }

  public static byte[] bitwiseZip(int x, int y) {
    byte[] ret = new byte[8];
    int xh = makeGap(x);
    int xl = makeGap(x << 16);
    int yh = makeGap(y) >>> 1;
    int yl = makeGap(y << 16) >>> 1;

    int zh = xh | yh;
    int zl = xl | yl;

    byte[] rh = Bytes.toBytes(zh);
    byte[] rl = Bytes.toBytes(zl);
    System.arraycopy(rh, 0, ret, 0, 4);
    System.arraycopy(rl, 0, ret, 4, 4);
    return ret;
  }

  private static final int[] MASKS = new int[] { 0xFFFF0000, 0xFF00FF00,
      0xF0F0F0F0, 0xCCCCCCCC, 0xAAAAAAAA };

  public static int makeGap(int x) {
    int x0 = x & MASKS[0];
    int x1 = (x0 | (x0 >>> 8)) & MASKS[1];
    int x2 = (x1 | (x1 >>> 4)) & MASKS[2];
    int x3 = (x2 | (x2 >>> 2)) & MASKS[3];
    int x4 = (x3 | (x3 >>> 1)) & MASKS[4];
    return x4;
  }

  public static int[] bitwiseUnzip(byte[] bs) {
    int zh = Bytes.toInt(bs, 0);
    int zl = Bytes.toInt(bs, 4);

    int xh = elimGap(zh);
    int yh = elimGap(zh << 1);
    int xl = elimGap(zl) >>> 16;
    int yl = elimGap(zl << 1) >>> 16;

    int x = xh | xl;
    int y = yh | yl;
    return new int[] { x, y };
  }

  public static int elimGap(int x) {
    int x0 = x & MASKS[4];
    int x1 = (x0 | (x0 << 1)) & MASKS[3];
    int x2 = (x1 | (x1 << 2)) & MASKS[2];
    int x3 = (x2 | (x2 << 4)) & MASKS[1];
    int x4 = (x3 | (x3 << 8)) & MASKS[0];
    return x4;
  }

  public static byte[] concat(byte[] b1, byte[] b2) {
    checkNotNull(b1);
    checkNotNull(b2);

    byte[] ret = new byte[b1.length + b2.length];
    System.arraycopy(b1, 0, ret, 0, b1.length);
    System.arraycopy(b2, 0, ret, b1.length, b2.length);
    return ret;
  }

  public static boolean prefixMatch(byte[] prefix, int prefixSize, byte[] target) {
    byte[] mask = makeMask(prefixSize);
    checkArgument(prefix.length >= mask.length);
    checkArgument(target.length >= mask.length);

    for (int i = 0; i < mask.length; i++) {
      if (!(prefix[i] == (target[i] & mask[i]))) {
        return false;
      }
    }
    return true;
  }

  public static byte[] makeMask(int prefixSize) {
    checkArgument(prefixSize > 0);
    int d = (prefixSize - 1) / 8;
    int r = (prefixSize - 1) % 8;
    // mask[] = {0b10000000, 0b11000000, ..., 0b11111111}
    final byte[] mask = new byte[] { -128, -64, -32, -16, -8, -4, -2, -1 };

    byte[] ret = new byte[8];
    for (int i = 0; i < d; i++) {
      ret[i] = -1; // 0xFF
    }
    ret[d] = mask[r];
    return ret;
  }

  public static byte[] not(byte[] bs) {
    byte[] ret = new byte[bs.length];
    for (int i = 0; i < bs.length; i++) {
      ret[i] = (byte) (~bs[i]);
    }
    return ret;
  }

  public static byte[] or(byte[] b1, byte[] b2) {
    checkArgument(b1.length == b2.length);
    byte[] ret = new byte[b1.length];
    for (int i = 0; i < b1.length; i++) {
      ret[i] = (byte) (b1[i] | b2[i]);
    }
    return ret;
  }

  public static byte[] and(byte[] b1, byte[] b2) {
    checkArgument(b1.length == b2.length);
    byte[] ret = new byte[b1.length];
    for (int i = 0; i < b1.length; i++) {
      ret[i] = (byte) (b1[i] & b2[i]);
    }
    return ret;
  }

  public static byte[] makeBit(byte[] target, int pos) {
    checkArgument(pos >= 0);
    checkArgument(pos < target.length * 8);
    int d = pos / 8;
    int r = pos % 8;
    final byte[] bits = new byte[] { -128, 64, 32, 16, 8, 4, 2, 1 };

    byte[] ret = new byte[target.length];
    System.arraycopy(target, 0, ret, 0, target.length);
    ret[d] |= bits[r];
    return ret;
  }

  public static String toString(byte[] key, int prefixLength) {
    StringBuilder buf = new StringBuilder();
    int d = (prefixLength - 1) / 8;
    int r = (prefixLength - 1) % 8;

    final int[] masks = new int[] { -128, 64, 32, 16, 8, 4, 2, 1 };
    for (int i = 0; i < d; i++) {
      for (int j = 0; j < masks.length; j++) {
        buf.append((key[i] & masks[j]) == 0 ? "0" : "1");
      }
    }
    for (int j = 0; j <= r; j++) {
      buf.append((key[d] & masks[j]) == 0 ? "0" : "1");
    }
    for (int j = r + 1; j < masks.length; j++) {
      buf.append("*");
    }
    for (int i = d + 1; i < key.length; i++) {
      buf.append("********");
    }
    return buf.toString();
  }
}
