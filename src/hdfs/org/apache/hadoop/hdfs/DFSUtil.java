/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs;

import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.net.NodeBase;

public class DFSUtil {
  /**
   * Whether the pathname is valid.  Currently prohibits relative paths, 
   * and names which contain a ":" or "/" 
   */
  public static boolean isValidName(String src) {
      
    // Path must be absolute.
    if (!src.startsWith(Path.SEPARATOR)) {
      return false;
    }
      
    // Check for ".." "." ":" "/"
    StringTokenizer tokens = new StringTokenizer(src, Path.SEPARATOR);
    while(tokens.hasMoreTokens()) {
      String element = tokens.nextToken();
      if (element.equals("..") || 
          element.equals(".")  ||
          (element.indexOf(":") >= 0)  ||
          (element.indexOf("/") >= 0)) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Converts a byte array to a string using UTF8 encoding.
   */
  public static String bytes2String(byte[] bytes) {
    try {
      return new String(bytes, "UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Converts a string to a byte array using UTF8 encoding.
   */
  public static byte[] string2Bytes(String str) {
    try {
      return str.getBytes("UTF8");
    } catch(UnsupportedEncodingException e) {
      assert false : "UTF8 encoding is not supported ";
    }
    return null;
  }

  /**
   * Convert a LocatedBlocks to BlockLocations[]
   * @param blocks a LocatedBlocks
   * @return an array of BlockLocations
   */
  public static BlockLocation[] locatedBlocks2Locations(LocatedBlocks blocks) {
    if (blocks == null) {
      return new BlockLocation[0];
    }
    int nrBlocks = blocks.locatedBlockCount();
    BlockLocation[] blkLocations = new BlockLocation[nrBlocks];
    int idx = 0;
    for (LocatedBlock blk : blocks.getLocatedBlocks()) {
      assert idx < nrBlocks : "Incorrect index";
      DatanodeInfo[] locations = blk.getLocations();
      String[] hosts = new String[locations.length];
      String[] names = new String[locations.length];
      String[] racks = new String[locations.length];
      for (int hCnt = 0; hCnt < locations.length; hCnt++) {
        hosts[hCnt] = locations[hCnt].getHostName();
        names[hCnt] = locations[hCnt].getName();
        NodeBase node = new NodeBase(names[hCnt], 
                                     locations[hCnt].getNetworkLocation());
        racks[hCnt] = node.toString();
      }
      blkLocations[idx] = new BlockLocation(names, hosts, racks,
                                            blk.getStartOffset(),
                                            blk.getBlockSize());
      idx++;
    }
    return blkLocations;
  }

  /** Create a URI from the scheme and address */
  public static URI createUri(String scheme, InetSocketAddress address) {
    try {
      return new URI(scheme, null, address.getHostName(), address.getPort(),
          null, null, null);
    } catch (URISyntaxException ue) {
      throw new IllegalArgumentException(ue);
    }
  }
  
  /*
   * @CPSC438
   * Takes a filename and sorts it, by writing out to different files 
   * and then merges them simultaneously
   * Also takes a separator to indicate how to find columns, and int
   * to indicate which column to sort. 
   */
  public static void mergeFile(String filename, String sep, Int col) {
    DataInputStream fs = new DataInputStream( 
  }
  
  long MIN_MEMORY = //Large Number;

  public enum ColDataType {
      STRING, INTEGER
  };

  /* 
   * @CS438
   * sep is the separator character, col is the column number. 
   */
  void sortBlock(InputStream block, String sep, int col, ColDataType type)
  {  

    long fileNum = 0;

    BufferedReader in = new BufferedReader(new InputStreamReader(block));
  
    ArrayList<String> lines = new ArrayList<String>();
    long freeMemory = Runtime.freeMemory();
  
    int counter = 0;
    while((inputLine = in.readLine()) != null) 
    {
      while((freeMemory = Runtime.freeMemory()) > MIN_MEMORY) 
        lines.add(inputLine, counter++);    

      int len = lines.size();
      String[] lineArray = new String[len];
      lines.toArray(lineArray);
      InputCompare<String> comp = new InputCompare<String>(sep, col, type);
      Arrays.sort(lineArray, comp);
    
      Daemon d = new Daemon(new ConcurrentSortedWriter(someFilename + fileNum++, lineArray));
      d.start();
      lines.clear();
      System.gc();
      counter = 0;
    }
    in.close();
  }

  public class ConcurrentSortedWriter implements Runnable{
  
    String filename;
    String[] arr;

    public ConcurrentSortedWriter(String filename, String[] arr) {
      this.filename = filename;
      this.arr = arr;
    }
    
    public void run() {
      PrintWriter out = new PrintWriter(new FileWriter(filename));
      for(int i = 0; i < arr.length; i++)
        out.println(arr[i]);
      out.close();
    }
  }

  public class InputCompare<String> implements Comparator<String> {
  
    String separator = ",";
    int col = 0;
    ColDataType type = STRING;
  
    public InputCompare<String>(String separator, int col, ColDataType type) {
      this.separator = separator;
      this.col = col;
      this.type = type;
    }

    public int compare(String o1, String o2) {
      String s1 = o1.split(separator)[col];
      String s2 = o2.split(separator)[col];
      
      switch (type) {
        case INTEGER:
          return Integer.parseInt(s1).compareTo(s2);
        case STRING:
        default:
          return s1.compareTo(s2);
      }
    }
  
    public boolean equals(String o1, String o2) {
      return compare(o1, o2) == 0;
    }
  }

  /*private void quicksort(ArrayList<Comparable> a) {
    return quicksorthelper(a, 0, a.size()-1);
  }

  private void quicksorthelper(ArrayList<Comparable> a, int startIndex, int endIndex)
	{
		if(startIndex<endIndex)
	 	{
	 		int pivot = partition(a, startIndex, endIndex);
	 		quicksorthelper(a, startIndex, pivot);
	 		if(pivot!=endIndex) quicksorthelper(a, pivot+1, endIndex);
	 	}
	 	else
	 	{
	 	}
	 }

  //Postcondition:  Returns the index of the pivot element.
  //                All elements on the left side of the pivot (from lowIndex)
  //                are less than or equal to the pivot.
	//                All elements on the right side of the pivot (through highIndex)
	//                are greater than or equal to the pivot.
	private int partition(ArrayList<Comparable> a, int lowIndex, int highIndex)
	{
		int pivot = lowIndex;
		for (int unsorted = lowIndex + 1; unsorted <= highIndex; unsorted++)
		{
			if (a[unsorted].compareTo(a[pivot]) < 0)
			{
				Comparable temp = a[pivot];
				a[pivot] = a[unsorted];
				display.update();
				a[unsorted] = a[pivot + 1];
				display.update();
				a[pivot + 1] = temp;
				display.update();
				pivot++;
			}
		}

		return pivot;
	}*/
}

