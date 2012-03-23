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

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Iterables;
import com.google.common.io.Closeables;

/**
 * Tiny MD-HBase Client
 * 
 * @author shoji
 * 
 */
public class Client implements Closeable {

  private final Index index;

  public Client(String tableName, int splitThreshold) throws IOException {
    this.index = new Index(HBaseConfiguration.create(), tableName,
        splitThreshold);
  }

  public void insert(Point p) throws IOException {
    byte[] row = Utils.bitwiseZip(p.x, p.y);
    Bucket bucket = index.fetchBucket(row);
    bucket.insert(row, p);
  }

  public Iterable<Point> get(int x, int y) throws IOException {
    byte[] row = Utils.bitwiseZip(x, y);
    Bucket bucket = index.fetchBucket(row);
    return bucket.get(row);
  }

  /**
   * 
   * @param rx
   *          a query range on dimension x
   * @param ry
   *          a query range on dimension y
   * @return points within the query region
   * @throws IOException
   */
  public Iterable<Point> rangeQuery(Range rx, Range ry) throws IOException {
    Iterable<Bucket> buckets = index.findBucketsInRange(rx, ry);
    List<Point> results = new LinkedList<Point>();
    for (Bucket bucket : buckets) {
      results.addAll(bucket.scan(rx, ry));
    }
    return results;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Closeable#close()
   */
  @Override
  public void close() throws IOException {
    index.close();
  }

  /**
   * 
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    Client client = new Client("Sample", 10);

    try {
      if (args.length == 0) {
        showHelp();
      } else if (args[0].equals("put")) {
        int id;
        if (args.length < 4) {
          Random idGenerator = new Random(System.nanoTime());
          id = idGenerator.nextInt();
        } else {
          id = Integer.parseInt(args[3]);
        }
        int x = Integer.parseInt(args[1]);
        int y = Integer.parseInt(args[2]);
        Point p = new Point(id, x, y);
        client.insert(p);
      } else if (args[0].equals("get")) {
        int x = Integer.parseInt(args[1]);
        int y = Integer.parseInt(args[2]);
        Iterable<Point> points = client.get(x, y);
        for (Point point : points) {
          System.out.println(point);
        }
      } else if (args[0].equals("count")) {
        int xmin = Integer.parseInt(args[1]);
        int ymin = Integer.parseInt(args[2]);
        int xmax = Integer.parseInt(args[3]);
        int ymax = Integer.parseInt(args[4]);
        System.out.println(String.format("Query Region: [(%d,%d), (%d,%d)]",
            xmin, ymin, xmax, ymax));
        Iterable<Point> points = client.rangeQuery(new Range(xmin, xmax),
            new Range(ymin, ymax));
        System.out.println(String.format("%d hits", Iterables.size(points)));
      } else if (args[0].equals("index")) {
        HTable index = new HTable("Sample_index");
        System.out.println("bucket name: size");
        ResultScanner entries = index.getScanner(Index.FAMILY_INFO);
        for (Result entry : entries) {
          byte[] key = entry.getRow();
          int prefixLength = Bytes.toInt(entry.getValue(Index.FAMILY_INFO,
              Index.COLUMN_PREFIX_LENGTH));
          long bucketSize = Bytes.toLong(entry.getValue(Index.FAMILY_INFO,
              Index.COLUMN_BUCKET_SIZE));
          System.out.println(String.format("%s: %d",
              Utils.toString(key, prefixLength), bucketSize));
        }
      } else if (args[0].equals("drop")) {
        client.close();
        HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
        admin.disableTable("Sample_index");
        admin.deleteTable("Sample_index");
        admin.disableTable("Sample");
        admin.deleteTable("Sample");
        admin.close();
      } else {
        showHelp();
      }
    } finally {
      Closeables.closeQuietly(client);
    }
  }

  private static void showHelp() {
    StringBuilder buf = new StringBuilder();
    buf.append("Usage: \n");
    buf.append(" put x y [id]\tput an entity at (x,y)\n");
    buf.append(" get x y\tget points at (x,y)\n");
    buf.append(" count xmin ymin xmax ymax\tcount # of points within region[(xmin,ymin),(xmax,ymax)]\n");
    buf.append(" index\tshow the index entries\n");
    buf.append(" drop\tdrop tables\n");
    System.out.println(buf.toString());
  }

}
