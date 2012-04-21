import java.util.*;
import java.io.*;
import java.lang.Object;

public class eSort {
	public enum ColDataType {
		STRING, INTEGER
	}

	public static void main(String[] args) throws Exception {
		String fileName = args[0];
		int column = Integer.parseInt(args[1]);
		ColDataType cdt = Integer.parseInt(args[2]) > 0 ? ColDataType.INTEGER : ColDataType.STRING;
		Process pr = null;

		switch (cdt) {
			case STRING:
				pr = Runtime.getRuntime().exec("/Users/aayushu/Desktop/sort.sh " + fileName + " " + column + " 0");
				pr.waitFor();
				 
			break;
			case INTEGER:
				pr = Runtime.getRuntime().exec("/Users/aayushu/Desktop/sort.sh " + fileName + " " + column + " 1");
				pr.waitFor();
			break;
		}
	}	
}

