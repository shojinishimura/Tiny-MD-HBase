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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * @author shoji
 * 
 */
public class UtilsTest {

  @Test
  public void testBitwiseZip() {
    int x = 0x0000FFFF;
    int y = 0x00FF00FF;
    byte[] actual = Utils.bitwiseZip(x, y);
    byte[] expected = new byte[] { 0x00, 0x00, 0x55, 0x55, -0x55 - 1,
        -0x55 - 1, -0x00 - 1, -0x00 - 1 };
    assertArrayEquals(expected, actual);
  }

  @Test
  public void testBitwiseUnzip() throws Exception {
    byte[] data = Utils.bitwiseZip(0x0000FFFF, 0x00FF00FF);
    int[] actual = Utils.bitwiseUnzip(data);
    assertArrayEquals(new int[] { 0x0000FFFF, 0x00FF00FF }, actual);
  }

  @Test
  public void testMakegap0() throws Exception {
    int x = 0x0000FFFF; // 0b00..0011..11
    int actual = Utils.makeGap(x); // 0b1010...10
    assertEquals(0, actual);
  }

  @Test
  public void testMakegap1() throws Exception {
    int x = 0xFFFF0000; // 0b11..1100..00
    int actual = Utils.makeGap(x); // 0b1010...10
    assertEquals(0xAAAAAAAA, actual);
  }

  @Test
  public void testMakeMask() throws Exception {
    byte[] actual1 = Utils.makeMask(1);
    assertArrayEquals(new byte[] { -128, 0, 0, 0, 0, 0, 0, 0 }, actual1);

    byte[] actual2 = Utils.makeMask(8);
    assertArrayEquals(new byte[] { -1, 0, 0, 0, 0, 0, 0, 0 }, actual2);

    byte[] actual3 = Utils.makeMask(9);
    assertArrayEquals(new byte[] { -1, -128, 0, 0, 0, 0, 0, 0 }, actual3);
  }
}
