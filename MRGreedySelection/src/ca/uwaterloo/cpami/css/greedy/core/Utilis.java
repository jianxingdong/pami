package ca.uwaterloo.cpami.css.greedy.core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class Utilis {

	public static double[][] loadMatrix(String path, String delm) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
		String line = null;
		int m = 0;
		int n = 0;
		ArrayList<String> lines = new ArrayList<String>();
		while ((line = bufferedReader.readLine()) != null) {
			lines.add(line);
		}
		m = lines.size();
		if (m == 0) {
			return new double[0][0];
		} else {
			n = lines.get(0).split(delm).length;
		}
		double[][] matrix = new double[m][n];
		int i = 0;
		for (String l : lines) {
			String[] split = l.split(delm);
			double[] row = new double[n];
			for (int j = 0; j < n; j++) {
				row[j] = Double.parseDouble(split[j]);
			}
			matrix[i++] = row;
		}
		return matrix;
	}
}
