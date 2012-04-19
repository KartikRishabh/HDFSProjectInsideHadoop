import java.util.*;
import java.io.*;


public class CopyArray {
	
	public static void main(String[] args) throws Exception{
		
		ArrayList<String> lines = new ArrayList<String>();
		String inputLine;
		BufferedReader in = new BufferedReader(new FileReader(args[0]));
		while((inputLine = in.readLine())!=null) {
			lines.add(inputLine);
		}
		in.close();
		
		long time = System.currentTimeMillis();
		Object[] lineArray = lines.toArray();
		System.out.println( System.currentTimeMillis()-time);
	}
}
