package org.apache.storm.starter.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReadStopWords {
	public static String[] read(String filePath) {
		BufferedReader in;
		List<String> list = new ArrayList<String>();
		try {
			in = new BufferedReader(new FileReader(filePath));
			String str;

			list = new ArrayList<String>();
			while ((str = in.readLine()) != null) {
				list.add(str);
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
		String[] stringArr = list.toArray(new String[0]);
		return stringArr;
	}
}
