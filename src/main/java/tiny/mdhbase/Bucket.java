/*
 *    Copyright 2012 Shoji Nishimura
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package tiny.mdhbase;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Bucket is a container of points. Each bucket is associated with a sub-space
 * organized by Index.
 * 
 * Schema:
 * <ul>
 * <li>row key: bitwise zip of {@link Point#x} and {@link Point#y}. ex.
 * x=[x0,x1,..,x31], y=[y0,y1,..,y31] -> [x0,y0,x1,y1,..,x31,y31]
 * <li>column name: "P:" + byte array of {@link Point#id}
 * <li>column value: concatination of byte arrays of {@link Point#x} and
 * {@link Point#y}
 * </ul>
 * 
 * @author shoji
 * 
 */
public class Bucket {

  public static byte[] FAMILY = "P".getBytes();

  private final HTable dataTable;
  private final byte[] startRow;
  private final byte[] stopRow;
  private final Index index;
  private final Range rangeX;
  private final Range rangeY;

  public Bucket(HTable dataTable, Range rx, Range ry, Index index) {
    checkNotNull(dataTable);
    checkNotNull(rx);
    checkNotNull(ry);
    checkNotNull(index);
    this.dataTable = dataTable;
    this.rangeX = rx;
    this.rangeY = ry;
    this.startRow = Utils.bitwiseZip(rx.min, ry.min);
    this.stopRow = Bytes.incrementBytes(Utils.bitwiseZip(rx.max, ry.max), 1L);
    this.index = index;
  }

  public void insert(byte[] row, Point p) throws IOException {
    Put put = new Put(row);
    put.add(FAMILY, toQualifier(p), toValue(p));
    dataTable.put(put);
    index.notifyInsertion(row);
  }

  /**
   * gets points at the query points
   * 
   * @param row
   * @return
   * @throws IOException
   */
  public Collection<Point> get(byte[] row) throws IOException {
    Get get = new Get(row);
    get.addFamily(FAMILY);
    Result result = dataTable.get(get);

    List<Point> found = new LinkedList<Point>();
    transformResultAndAddToList(result, found);
    return found;
  }

  /**
   * scans this bucket and retrieves all points within the query region.
   * 
   * @param rx
   *          a query range on dimension x
   * @param ry
   *          a query range on dimension y
   * @return a collection of points within the query region
   * @throws IOException
   */
  public Collection<Point> scan(Range rx, Range ry) throws IOException {
    Scan scan = new Scan(startRow, stopRow);
    Filter filter = new RangeFilter(rx, ry);
    scan.setFilter(filter);
    scan.setCaching(1000);
    ResultScanner scanner = dataTable.getScanner(scan);
    List<Point> results = new LinkedList<Point>();
    for (Result result : scanner) {
      transformResultAndAddToList(result, results);
    }
    return results;
  }

  public Collection<Point> scan() throws IOException {
    return scan(rangeX, rangeY);
  }

  private void transformResultAndAddToList(Result result, List<Point> found) {
    NavigableMap<byte[], byte[]> map = result.getFamilyMap(FAMILY);
    for (Entry<byte[], byte[]> entry : map.entrySet()) {
      Point p = toPoint(entry.getKey(), entry.getValue());
      found.add(p);
    }
  }

  private byte[] toQualifier(Point p) {
    return Bytes.toBytes(p.id);
  }

  private byte[] toValue(Point p) {
    byte[] bx = Bytes.toBytes(p.x);
    byte[] by = Bytes.toBytes(p.y);
    return Utils.concat(bx, by);
  }

  private Point toPoint(byte[] qualifier, byte[] value) {
    long id = Bytes.toLong(qualifier);
    int x = Bytes.toInt(value, 0);
    int y = Bytes.toInt(value, 4);
    return new Point(id, x, y);
  }

  public double distanceFrom(Point point) {
    double dx = rangeX.distanceFrom(point.x);
    double dy = rangeY.distanceFrom(point.y);
    return Math.sqrt(dx * dx + dy * dy);
  }

  public Point farthestCornerFrom(Point point) {
    final int DUMMY = -1;
    return new Point(DUMMY, rangeX.farthestFrom(point.x),
        rangeY.farthestFrom(point.y));
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return String.format("Bucket[(%d,%d), (%d,%d)]", rangeX.min, rangeY.min,
        rangeX.max, rangeY.max);
  }
}
