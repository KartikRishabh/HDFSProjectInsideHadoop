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

import java.io.*;
//BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.net.NodeBase;

import org.apache.commons.logging.*;

public class DFSUtil {

  public static final Log LOG = LogFactory.getLog(DFSClient.class);

  /**
   * @CPSC438
   * Indicates where the external sort program is located
   */
  public static String EXTERNAL_SORT = "/home/accts/krv6/bin/sort.sh";

  /**
   * @CPSC438
   * Enum to indicate various types of datat types in columns
   */
  public enum ColDataType {
    INTEGER, STRING
  }  
  
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
   * Calls an external sort progam.
   * Also takes a separator to indicate how to find columns, and int
   * to indicate which column to sort. 
   */
  public static void sortFile(String filename, int column) {

    LOG.info("Filename: " + filename);
    LOG.info("S Column: " + column);
  
    try {
		  Process pr = null;
		  String runCommand = DFSUtil.EXTERNAL_SORT + " " + filename + " " + 
		                    column + " ";
		                    
		  ColDataType cdt = ColDataType.INTEGER;
    
      BufferedReader in = null;
      in = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
                              
		  String inputLine = in.readLine();
      if(inputLine != null && 
         inputLine.split(",")[column-1].replaceAll("\\d+", "").length() > 0) {
        cdt = ColDataType.STRING;
      }
      in.close();
		  
		  switch (cdt) {
		    case INTEGER:
		    	pr = Runtime.getRuntime().exec(runCommand + 1);
			   	pr.waitFor();
		   	  break;
		    	
		   	case STRING:
		   	default:
		   		pr = Runtime.getRuntime().exec(runCommand + 0);
			   	pr.waitFor();
			   break;
		  }
		  LOG.info("Really, trully, did finish sorting");
		}
		catch(Exception e) {
		  LOG.info("What the hell is going on!");
		  e.printStackTrace();
		}
	  LOG.info("Bye!");
	}
}
