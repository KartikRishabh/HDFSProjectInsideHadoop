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

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


import org.apache.hadoop.hdfs.DFSUtil;

/**
 * Treats keys as offset in file and value as line. 
 */
public class LineRecordReader implements RecordReader<LongWritable, Text> {
  private static final Log LOG
    = LogFactory.getLog(LineRecordReader.class.getName());

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  int maxLineLength;

	private LongWritable mKey;
	private Text mValue;

  /**
   * A class that provides a line reader from an input stream.
   * @deprecated Use {@link org.apache.hadoop.util.LineReader} instead.
   */
  @Deprecated
  public static class LineReader extends org.apache.hadoop.util.LineReader {
    LineReader(InputStream in) {
      super(in);
    }
    LineReader(InputStream in, int bufferSize) {
      super(in, bufferSize);
    }
    public LineReader(InputStream in, Configuration conf) throws IOException {
      super(in, conf);
    }
  }

  public LineRecordReader(Configuration job, 
                          FileSplit split) throws IOException {
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    
    // @CPSC438
    //FSDataInputStream fileIn = fs.open(split.getPath());
    FSDataInputStream fileIn = fs.open(split.getPath());//DFSUtil.blockReduce(8192, fs.open(split.getPath()), 4, "above", "below");
    
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
		nextUntilStart(3, "5000", "5099", true); // @CPSC438
  }

	/**
	 * @CPSC438
   */
	public boolean nextUntilStart(int col, String startValue, String endValue, boolean isInt) {
		
		/*key.set(pos);
		if(value.toString().split(",")[3].compareTo("below") > 0) {
			pos = end;
			return false;
		}*/
		
		long lstart = Long.parseLong(startValue);
		long lend = Long.parseLong(endValue);
		long target;
		
		LongWritable key = createKey();
		Text value = createValue();
		
		try {
			String[] splits;
    	while (pos < end) {

      	key.set(pos);

      	int newSize = in.readLine(value, maxLineLength,
                                Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                         maxLineLength));
			
			  // @CPSC438
      	if (newSize == 0) {
        	return false;
     	 	}
			
				splits = value.toString().split(",");
				if (splits.length >= 3) {
				  target = Long.parseLong(splits[col-1]);
				  if (target > lend) {
				    pos = end;
				    return false;
				  }
				  if (target >= lstart) {
				    LOG.info("First next() value: " + value);
				    return true;
				  }
				}
 				/*if(splits[col-1].compareTo(endValue) > 0) {
					pos = end;
					return false;
				}
				if(splits[col-1].compareTo(startValue) >= 0) {
					//LOG.info("First next() value: " + value);
					return true;
				}*/
	
  	    pos += newSize;
    	}
    	return false;
		} catch(Exception e) {
			LOG.info(e.toString());
			return false;
		}
  }

  public LineRecordReader(InputStream in, long offset, long endOffset,
                          int maxLineLength) {
    this.maxLineLength = maxLineLength;
    /**
     * @CPSC438
     */
    //LOG.info("Constructor 1");
    //this.in = new LineReader(DFSUtil.blockReduce(512, in, 4, "above", "below"));
    this.in = new LineReader(in);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
  }

  public LineRecordReader(InputStream in, long offset, long endOffset, 
                          Configuration job) 
    throws IOException{
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                                    Integer.MAX_VALUE);
    /**
     * @CPSC438
     */
  //  LOG.info("Constructor 2");
    //this.in = new LineReader(DFSUtil.blockReduce(512, in, 4, "above", "below"), job);
    this.in = new LineReader(in, job);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;    
  }
  
  public LongWritable createKey() {
    return new LongWritable();
  }
  
  public Text createValue() {
    return new Text();
  }
  
  /** Read a line. */
  public synchronized boolean next(LongWritable key, Text value)
    throws IOException {

    while (pos < end) {
    
      // @CPSC438
      //String text = value.toString();
      //text = text.substring( 

      key.set(pos);

      int newSize = in.readLine(value, maxLineLength,
                                Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                         maxLineLength));

			String[] splits = value.toString().split(",");
			long target;
			if(splits.length > 2) { 
			  target = Long.parseLong(splits[2]);
			  if (target > 5099)
			    return false;
		  }

      if (newSize == 0) {
        return false;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        return true;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
    }

    return false;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public  synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}
