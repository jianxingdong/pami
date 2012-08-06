package ca.uwaterloo.cpami.css.greedy.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.math3.linear.Array2DRowRealMatrix;

public class GreedyColSubsetSelection {

	/**
	 * 
	 * @param k
	 *            num cols to select
	 * @return the indices of the selected columns (indicies start from 0)
	 * @throws Exception
	 */
	public Integer[] selectColumnSubset(Array2DRowRealMatrix sourceMat,
			Array2DRowRealMatrix targetMat, int k) {

		Array2DRowRealMatrix G = (Array2DRowRealMatrix) targetMat.transpose()
				.multiply(sourceMat); // pxn
		int p = G.getRowDimension();
		int n = G.getColumnDimension();
		int m = sourceMat.getRowDimension();

		Array2DRowRealMatrix f = new Array2DRowRealMatrix(n, 1); // nx1
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < p; j++) {
				f.setEntry(i, 0,
						f.getEntry(i, 0) + G.getEntry(i, j) * G.getEntry(i, j));
			}
		}

		Array2DRowRealMatrix g = new Array2DRowRealMatrix(n, 1); // nx1
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < m; j++) {
				g.setEntry(i, 0, g.getEntry(i, 0) + sourceMat.getEntry(j, i)
						* sourceMat.getEntry(j, i));
			}
		}

		ArrayList<Integer> selectedIndices = new ArrayList<Integer>(k);
		HashSet<Integer> selectedIndicesHash = new HashSet<Integer>();
		Array2DRowRealMatrix W = new Array2DRowRealMatrix(n, k); // nxk
		Array2DRowRealMatrix V = new Array2DRowRealMatrix(p, k); // pxk

		for (int t = 0; t < k; t++) {
			int l = findMaxRatio(f.getColumn(0), g.getColumn(0), n,
					selectedIndicesHash);
			if (l == -1) {
				break;
			}
			selectedIndices.add(l);
			selectedIndicesHash.add(l);
			// modify f and g
			Array2DRowRealMatrix delta = (Array2DRowRealMatrix) sourceMat
					.transpose().multiply(
							sourceMat.getSubMatrix(0, m - 1, l, l)); // nx1

			Array2DRowRealMatrix gamma = (Array2DRowRealMatrix) G.getSubMatrix(
					0, n - 1, l, l); // px1
			if (t > 0) {
				delta = (Array2DRowRealMatrix) delta.subtract(W.getSubMatrix(0,
						n - 1, 0, t - 1).multiply(
						W.getSubMatrix(l, l, 0, t - 1).transpose()));

				gamma = (Array2DRowRealMatrix) gamma.subtract(V.getSubMatrix(0,
						p - 1, 0, t - 1).multiply(
						W.getSubMatrix(l, l, 0, t - 1).transpose()));

			}

			double alphaSqrt = Math.sqrt(delta.getEntry(l, 0));
			Array2DRowRealMatrix w = (Array2DRowRealMatrix) delta
					.scalarMultiply(1.0 / alphaSqrt);// nx1
			W.setSubMatrix(w.getData(), 0, t);

			Array2DRowRealMatrix v = (Array2DRowRealMatrix) gamma
					.scalarMultiply(1.0 / alphaSqrt);// px1
			V.setSubMatrix(v.getDataRef(), 0, t);

			if (isExceededRank(delta, alphaSqrt))
				break;

			Array2DRowRealMatrix r1 = (Array2DRowRealMatrix) G.transpose()
					.multiply(v);// nx1
			Array2DRowRealMatrix r3 = getHadamardProduct(w, w); // nx1

			Array2DRowRealMatrix r2 = new Array2DRowRealMatrix(n, 1);
			if (t > 0) {
				r2 = (Array2DRowRealMatrix) W.getSubMatrix(0, n - 1, 0, t - 1)
						.multiply(
								(V.getSubMatrix(0, p - 1, 0, t - 1).transpose()
										.multiply(v)));
			}

			f = (Array2DRowRealMatrix) f.subtract(
					getHadamardProduct(w, r1.subtract(r2)).scalarMultiply(2))
					.add(r3.scalarMultiply(Math.pow(v.getColumnVector(0)
							.getNorm(), 2)));			

			f.setEntry(l, 0, 0);
			g = g.subtract(r3);
			g.setEntry(l, 0, 0);
			for (int i = 0; i < n; i++) {
				if (f.getEntry(i, 0) < 1e-10)
					f.setEntry(i, 0, 0);

				if (g.getEntry(i, 0) < 1e-10)
					g.setEntry(i, 0, 0);
			}
		}

		return selectedIndices.toArray(new Integer[] {});
	}

	/**
	 * 
	 * 
	 * @return -1 if maxScore = 0
	 */
	private int findMaxRatio(double[] f, double[] g, int n,
			HashSet<Integer> selectedBefore) {
		double currMax = Double.MIN_VALUE;
		int maxIndex = -1;
		for (int i = 0; i < n; i++) {
			if (selectedBefore.contains(i))
				continue;
			double r = f[i] / g[i];
			if (Double.isInfinite(r) || Double.isNaN(r))
				continue;
			if (r > currMax) {
				currMax = r;
				maxIndex = i;
			}
		}
		return (currMax == 0) ? -1 : maxIndex;
	}

	private Array2DRowRealMatrix getHadamardProduct(Array2DRowRealMatrix A,
			Array2DRowRealMatrix B) {

		// A and B must be of the same size
		int m = A.getRowDimension();
		int n = A.getColumnDimension();
		Array2DRowRealMatrix result = new Array2DRowRealMatrix(m, n);
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				result.setEntry(i, j, A.getEntry(i, j) * B.getEntry(i, j));
			}
		}
		return result;
	}

	private boolean isExceededRank(Array2DRowRealMatrix delta, double alphaSqrt) {
		// delta is nx1
		double sumDeltaSqr = 0;
		for (double i : delta.getColumn(0))
			sumDeltaSqr += i * i;
		return sumDeltaSqr < 1e-40 || alphaSqrt < 1e-40;
	}

	public static void main(String[] args) throws IOException {
		double[][] a = Utilis
				.loadMatrix(
						"/home/ahmed/Desktop/ICDM13/dataset/docword.nips.fullmatrix_1k.txt",
						",");
		Integer[] cols = new GreedyColSubsetSelection().selectColumnSubset(
				new Array2DRowRealMatrix(a), new Array2DRowRealMatrix(a), 10);
		for (int i = 0; i < 10; i++)
			System.out.print(cols[i] + " ");
	}

}