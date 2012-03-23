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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author shoji
 * 
 */
public class RangeFilter extends FilterBase {

  public RangeFilter() {

  }

  private Range rx;
  private Range ry;

  public RangeFilter(Range rx, Range ry) {
    this.rx = rx;
    this.ry = ry;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int xmin = in.readInt();
    int xmax = in.readInt();
    int ymin = in.readInt();
    int ymax = in.readInt();

    this.rx = new Range(xmin, xmax);
    this.ry = new Range(ymin, ymax);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
   */
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(rx.min);
    out.writeInt(rx.max);
    out.writeInt(ry.min);
    out.writeInt(ry.max);
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.hadoop.hbase.filter.FilterBase#filterKeyValue(org.apache.hadoop
   * .hbase.KeyValue)
   */
  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    byte[] value = kv.getValue();
    int x = Bytes.toInt(value, 0);
    int y = Bytes.toInt(value, 4);
    if (rx.include(x) && ry.include(y)) {
      return ReturnCode.INCLUDE;
    } else {
      return ReturnCode.NEXT_ROW;
    }
  }

}
