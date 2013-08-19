/*
 * Copyright 2012 Shoji Nishimura
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tiny.mdhbase;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author shoji
 * 
 */
public class Point {
  public final long id;
  public final int x;
  public final int y;

  public Point(long id, int x, int y) {
    checkArgument(0 <= x);
    checkArgument(0 <= y);

    this.id = id;
    this.x = x;
    this.y = y;
  }

  public double distanceFrom(Point that) {
    double dx = this.x - that.x;
    double dy = this.y - that.y;
    return Math.sqrt(dx * dx + dy * dy);
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return String.format("[%d, (%d,%d)]", id, x, y);
  }

}
