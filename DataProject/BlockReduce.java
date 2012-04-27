import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.PrintStream;
import java.io.FileWriter;

import java.util.ArrayList;

public class BlockReduce {

	public enum ColDataType {
    INTEGER, STRING
  }
	
	public static final int LINES_TO_READ = 512;

	public static void blockReduce(String filename, int col, 
																 String startValue, String endValue) 
																throws Exception{

    //LOG.info("Filename: " + filename);
    //LOG.info("S Column: " + column);
		
		ColDataType cdt = ColDataType.INTEGER;
    
    BufferedReader in = null;
    in = new BufferedReader(new FileReader(filename));
                              
		String inputLine = in.readLine();
    if(inputLine != null && 
       inputLine.split(",")[col-1].replaceAll("\\d+", "").length() > 0) {
      cdt = ColDataType.STRING;
			in.close();
    }
		else { 
    	in.close();
			blockReduce(filename, col, Long.parseLong(startValue), Long.parseLong(endValue));
			return ;
		}

		in = new BufferedReader(new FileReader(filename));
		//PrintStream out = System.out;//new PrintWriter(System.out);
		
		ArrayList<String> lines = new ArrayList<String>();
		inputLine = "";
		
		int count = 0;
		int i;
		String target;
		while ((inputLine = in.readLine())!=null && count < LINES_TO_READ) {
			lines.add(inputLine);
			if(count == LINES_TO_READ-1) {
				target = inputLine.split(",")[col-1];
				if (target.compareTo(startValue) >= 0 &&
						target.compareTo(endValue) <= 0) {
					System.out.println(inputLine);
					for(i = LINES_TO_READ-2; i >= 0; i--) {
						inputLine = lines.get(i);
						target = inputLine.split(",")[col-1];
						if (target.compareTo(startValue) >= 0 &&
								target.compareTo(endValue) <= 0)
							System.out.println(inputLine);
						else if (target.compareTo(startValue) < 0)
							break;
					}
				} else if (target.compareTo(endValue) > 0) {
					break;
				}
				count=-1;
				lines.clear();
			}
			count++;
		}
		in.close();
		int size = lines.size();
		for(i = 0; i < size; i++) {
			inputLine = lines.get(i);
			target = inputLine.split(",")[col-1];
			if (target.compareTo(endValue) <= 0 &&
					target.compareTo(startValue) >= 0)
				System.out.println(inputLine);
			else if (target.compareTo(endValue) > 0)
				break;
		}
		//out.close();
	}

	public static void blockReduce(String filename, int col, 
																 long startValue, long endValue) 
																throws Exception{

    //LOG.info("Filename: " + filename);
    //LOG.info("S Column: " + column);
		
		BufferedReader in = new BufferedReader(new FileReader(filename));
		PrintWriter out = new PrintWriter(new FileWriter(filename + "2"));
		
		ArrayList<String> lines = new ArrayList<String>();
		String inputLine = "";
		
		int count = 0;
		int i;
		long target;
		while ((inputLine = in.readLine())!=null && count < LINES_TO_READ) {
			lines.add(inputLine);
			if(count == LINES_TO_READ-1) {
				target = Long.parseLong(inputLine.split(",")[col-1]);
				if (target >= startValue && target <= endValue) {
					out.println(inputLine);
					for(i = LINES_TO_READ-2; i >= 0; i--) {
						inputLine = lines.get(i);
						target = Long.parseLong(inputLine.split(",")[col-1]);
						if (target >= startValue && target <= endValue)
							out.println(inputLine);
						else if (target < startValue)
							break;
					}
				} else if (target > endValue) {
					break;
				}
				count=-1;
  			lines.clear();
			}
			count++;
		}
		in.close();
		int size = lines.size();
		for(i = 0; i < size; i++) {
			inputLine = lines.get(i);
			target = Long.parseLong(inputLine.split(",")[col-1]);
			if (target <= endValue && target >= startValue)
				out.println(inputLine);
			else if (target > endValue)
				break;
		}
		out.close();
	}

	public static void main(String[] args) throws Exception {
		blockReduce(args[0], Integer.parseInt(args[1]), args[2], args[3]);
	}
}
