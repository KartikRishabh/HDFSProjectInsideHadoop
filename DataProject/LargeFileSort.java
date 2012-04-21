import java.util.*;
import java.io.*;

public class LargeFileSort {

	public static long MIN_MEMORY = 150000000;
	
	public enum ColDataType {
		INTEGER, STRINGS
	}

	/* 
	 * @CS438
   * sep is the separator character, col is the column number. 
   */
	public void sortBlock(String filename, String sep, int col, ColDataType type) throws Exception { 
		long fileNum = System.currentTimeMillis();

		Runtime r = Runtime.getRuntime();
		long freeMemory = r.freeMemory();

		//BufferedReader in = new BufferedReader(new InputStreamReader(block));
		BufferedReader in = new BufferedReader(new FileReader(filename));
		ArrayList<String> lines = new ArrayList<String>();
		String inputLine = "";
		
		int counter = 0;
		int len = 0;
		
		long[] times = new long[10];

		times[0] = System.currentTimeMillis();
	
		String target = "";	
		int index;
		while ((inputLine = in.readLine()) != null) {
			
			target = inputLine;	
			for(int i = 0; i < col; i++)
				target = target.substring(target.indexOf(sep)+1);
			index = target.indexOf(sep);
			if (index != -1)
				target = target.substring(0, index);
			lines.add(target + sep + inputLine);
			counter++;
			
			if(r.freeMemory() > MIN_MEMORY)
				continue ;
			
			//len = lines.size();
			String[] lineArray = new String[counter];;
			lines.toArray(lineArray);
			InputCompare<String> comp = new InputCompare<String>(sep, col, type);
			Arrays.sort(lineArray, comp);
			
			ConcurrentSortedWriter csw = new ConcurrentSortedWriter(filename+"s"+fileNum++, lineArray,sep);
			csw.start();
			lines.clear();
			System.gc();
			counter = 0;
			Thread.sleep(100);
			System.out.println(freeMemory -r.freeMemory());
		}
		times[1] = System.currentTimeMillis();
		times[0] = times[1]-times[0];
		System.out.println(r.freeMemory());
		System.out.println("Done reading");
		
		String[] lineArray = new String[counter];;
 		lines.toArray(lineArray); 
		times[2] = System.currentTimeMillis();
		times[1] = times[2] -times[1];
		System.out.println(r.freeMemory());
		System.out.println("Done copying");
		InputCompare<String> comp = new InputCompare<String>(sep, col, type);
		Arrays.sort(lineArray, comp);
		times[2] = System.currentTimeMillis()-times[2];
		System.out.println("Done sorting");
		ConcurrentSortedWriter csw = new ConcurrentSortedWriter(filename+"s"+fileNum++, lineArray, sep);
		csw.start();
		lines.clear();
		for(int i = 0; i < 3; i++)
			System.out.println("Time " + i + ": " + times[i]);
		System.out.println(freeMemory - r.freeMemory());
		in.close();
	}

	public class ConcurrentSortedWriter extends Thread { 
		String filename;
		String[] arr;
		String sep;
		
		public ConcurrentSortedWriter(String filename, String[] arr, String sep) {
			this.filename = filename;
			this.arr = arr;
			this.sep = sep;
		}

		public void run() {
			PrintWriter out = null; 
			try {
				out = new PrintWriter(new FileWriter(filename));
				for(int i = 0; i < arr.length; i++) {
					out.println(arr[i].substring(arr[i].indexOf(sep)+1));
				}
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
			finally {
				if(out != null)
					out.close();
			}
		}
	}

	public class InputCompare<T> implements Comparator<T> {
		
		private String separator;
		private int col = 0;
		private ColDataType type = ColDataType.STRINGS;

		public InputCompare(String separator, int col, ColDataType type) {
			this.separator = separator;
			this.col = col;
			this.type = type;
		}

		public int compare(T o1, T o2) {
			if (o1 instanceof String && o2 instanceof String) {
				String s1 = (String)o1, s2 = (String)o2;
				s1 = s1.substring(0, s1.indexOf(separator));
				s2 = s2.substring(0, s2.indexOf(separator));

				switch (type) {
					case INTEGER:
						return Integer.parseInt(s1) - Integer.parseInt(s2);
					case STRINGS:
					default:
						return s1.compareTo(s2);
				}
			}
			return 0;
		}

		public boolean equals(T o1, T o2) {
			return compare(o1, o2) == 0;
		}
	}

	public static void main(String[] args) throws Exception {
		Runtime r = Runtime.getRuntime();
		System.out.println(r.freeMemory());
		LargeFileSort lfs = new LargeFileSort();
		lfs.sortBlock(args[0], ",", Integer.parseInt(args[1]), 
		              Integer.parseInt(args[2]) > 0 ? ColDataType.INTEGER : ColDataType.STRINGS);
	}	
}
