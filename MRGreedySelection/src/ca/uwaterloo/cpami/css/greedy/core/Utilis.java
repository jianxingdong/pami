package ca.uwaterloo.cpami.css.greedy.core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.function.Functions;

public class Utilis {

	public static double[][] loadMatrix(String path, String delm)
			throws IOException {
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

	public static SparseMatrix loadSparseMatrix(String path, String delm,
			int m, int n) throws IOException {
		SparseMatrix mat = new SparseMatrix(m, n);
		BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
		String line = null;
		int curRow = 0;
		int nnz = 0;
		while ((line = bufferedReader.readLine()) != null) {
			String[] parts = line.split(delm);
			for (int i = 0; i < n; i++) {
				if (!parts[i].equals("0")) {
					mat.set(curRow, i, Double.parseDouble(parts[i]));
					nnz++;
				}
			}
			curRow++;
		}
		System.out.println("nnz: " + nnz);
		return mat;
	}

	public static Matrix partition(Matrix A, int numPartitions) {
		int m = A.numRows();
		int n = A.numCols();
		Matrix B = A.like(m, numPartitions);
		List<Integer> partitions = getRandomGroups(n, numPartitions);
		for (int col = 0; col < n; col++) {
			int colPart = partitions.get(col);
			B.assignColumn(
					colPart,
					B.viewColumn(colPart).assign(A.viewColumn(col),
							Functions.PLUS));
		}
		return B;
	}

	private static List<Integer> getRandomGroups(int n, int k) {
		List<Integer> groups = new ArrayList<Integer>();
		List<Integer> remaining = new ArrayList<Integer>();
		for (int i = 0; i < n; i++) {
			remaining.add(i);
			groups.add(0);
		}
		int g = 0;
		Random random = new Random();
		for (int i = n; i > 0; i--) {
			int index = random.nextInt(i);
			groups.set(remaining.get(index), g);
			remaining.remove(index);
			g = (g + 1) % k;
		}
		return groups;
	}

	/**
	 * 
	 * B'A:: B is the partitions matrix, A is much spare than A, Matrix stores
	 * the rows only in a sparse way, the columns are dense
	 */
	public static Matrix transposeTimes(Matrix B, Matrix A) {
		int m = B.numRows();
		int c = B.numCols();
		int n = A.numCols();
		SparseMatrix G = new SparseMatrix(c, n);
		double bElement = 0;
		for (int raInd = 0; raInd < m; raInd++) {
			Vector ra = A.viewRow(raInd);
			for (int cbInd = 0; cbInd < c; cbInd++) {
				if ((bElement = B.get(raInd, cbInd)) != 0) {
					Iterator<Vector.Element> raNNZItr = ra.iterateNonZero();
					while (raNNZItr.hasNext()) {
						Element elm = raNNZItr.next();
						G.set(cbInd, elm.index(), G.get(cbInd, elm.index())
								+ elm.get() * bElement);
					}
				}
			}
		}
		return G;
	}

	public static void main(String[] args) {
		SparseMatrix a = new SparseMatrix(3, 3);
		a.set(0, 0, 3);
		a.set(0, 1, 10);
		a.set(0, 2, 2);

		a.set(1, 0, 4);
		a.set(1, 1, 2);
		a.set(1, 2, 4);

		a.set(2, 0, 2);
		a.set(2, 1, 1);
		a.set(2, 2, 3);

		SparseMatrix b = new SparseMatrix(3, 2);
		b.set(0, 0, 1);
		b.set(0, 1, 2);

		b.set(1, 0, 2);
		b.set(1, 1, 2);

		b.set(2, 0, 2);
		b.set(2, 1, 1);

		Matrix g = calcATimeATranspose(a);
		for (int i = 0; i < g.numRows(); i++) {
			for (int j = 0; j < g.numCols(); j++) {
				System.out.println(g.get(i, j));
			}
		}
		/*
		 * Matrix G = transposeTimes(b, a); for(int i=0;i<4;i++) for(int
		 * j=0;j<6;j++) System.out.println(G.get(i, j));
		 * System.out.println(G.viewRow(1).getNumNondefaultElements());
		 */
	}

	public static Matrix multiply(Matrix A, Matrix B) {
		int ANumRows = A.numRows();
		int BNumColumns = B.numCols();
		Matrix result = A.like(ANumRows, BNumColumns);
		for (int i = 0; i < ANumRows; i++) {
			for (int j = 0; j < BNumColumns; j++) {
				Iterator<Vector.Element> rowI = A.viewRow(i).iterateNonZero();
				double sum = 0;
				while (rowI.hasNext()) {
					Element e = rowI.next();
					sum += e.get() * B.get(e.index(), j);
				}

				if (sum != 0) {
					result.set(i, j, sum);
				}
			}
		}
		return result;
	}

	public static Matrix calcATimeATranspose(Matrix A) {
		int ANumRows = A.numRows();

		Matrix result = A.like(ANumRows, ANumRows);
		for (int i = 0; i < ANumRows; i++) {
			for (int j = 0; j < ANumRows; j++) {
				Iterator<Vector.Element> rowI = A.viewRow(i).iterateNonZero();
				double sum = 0;
				while (rowI.hasNext()) {
					Element e = rowI.next();
					sum += e.get() * A.get(j, e.index());
				}

				if (sum != 0) {
					result.set(i, j, sum);
				}
			}
		}
		return result;
	}

}
