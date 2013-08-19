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

/**
 * @author shoji
 * 
 */
public class Range {
  public final int min;
  public final int max;

  /**
   * 
   * @param min
   *          inclusive
   * @param max
   *          inclusive
   */
  public Range(int min, int max) {
    checkArgument(min <= max, "min=%s must not be greater than max=%s", min,
        max);
    this.min = min;
    this.max = max;
  }

  public boolean include(int i) {
    return (min <= i) && (i <= max);
  }

  public boolean intersect(Range that) {
    return (this.min <= that.max) && (that.min <= this.max);
  }

  public int distanceFrom(int p) {
    if (include(p)) {
      return 0;
    } else {
      return Math.min(Math.abs(min - p), Math.abs(max - p));
    }
  }

  public int farthestFrom(int p) {
    if (Math.abs(min - p) > Math.abs(max - p)) {
      return min;
    } else {
      return max;
    }
  }
}
