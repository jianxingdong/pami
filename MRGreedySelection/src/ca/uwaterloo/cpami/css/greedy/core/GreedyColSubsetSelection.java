package ca.uwaterloo.cpami.css.greedy.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import Jama.Matrix;

public class GreedyColSubsetSelection {

	/**
	 * 
	 * @param k
	 *            num cols to select
	 * @return the indices of the selected columns (indicies start from 0)
	 * @throws Exception
	 */
	public Integer[] selectColumnSubset(double[][] source, double[][] target,
			int k) {
		Matrix sourceMat = new Matrix(source);
		Matrix targetMat = new Matrix(target);
		Matrix G = targetMat.transpose().times(sourceMat); // pxn
		int p = G.getRowDimension();
		int n = G.getColumnDimension();
		int m = sourceMat.getRowDimension();

		Matrix f = new Matrix(n, 1);// nx1
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < p; j++) {
				f.set(i, 0, f.get(i, 0) + G.get(i, j) * G.get(i, j));
			}
		}

		Matrix g = new Matrix(n, 1);// nx1
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < m; j++) {
				g.set(i, 0,
						g.get(i, 0) + sourceMat.get(j, i) * sourceMat.get(j, i));
			}
		}

		ArrayList<Integer> selectedIndices = new ArrayList<Integer>(k);
		HashSet<Integer> selectedIndicesHash = new HashSet<Integer>();
		Matrix W = new Matrix(n, k); // nxk
		Matrix V = new Matrix(p, k); // pxk

		for (int t = 0; t < k; t++) {
			int l = findMaxRatio(f.getColumnPackedCopy(),
					g.getColumnPackedCopy(), n, selectedIndicesHash);
			if (l == -1) {
				break;
			}
			selectedIndices.add(l);
			selectedIndicesHash.add(l);
			// modify f and g
			Matrix delta = sourceMat.transpose().times(
					sourceMat.getMatrix(0, m - 1, new int[] { l })); // nx1

			Matrix gamma = G.getMatrix(0, n - 1, new int[] { l }); // px1
			if (t > 0) {
				delta = delta.minusEquals(W.getMatrix(0, n - 1, 0, t - 1)
						.times(W.getMatrix(l, l, 0, t - 1).transpose()));

				gamma = gamma.minusEquals(V.getMatrix(0, p - 1, 0, t - 1)
						.times(W.getMatrix(l, l, 0, t - 1).transpose()));

			}

			double alphaSqrt = Math.sqrt(delta.get(l, 0));
			Matrix w = delta.times(1.0 / alphaSqrt);// nx1
			W.setMatrix(0, n - 1, new int[] { t }, w);

			Matrix v = gamma.times(1.0 / alphaSqrt);// px1
			V.setMatrix(0, p - 1, new int[] { t }, v);

			if (isExceededRank(delta, alphaSqrt))
				break;

			Matrix r1 = G.transpose().times(v);// nx1
			Matrix r3 = getHadamardProduct(w, w); // nx1

			Matrix r2 = new Matrix(n, 1);
			if (t > 0) {
				r2 = W.getMatrix(0, n - 1, 0, t - 1).times(
						(V.getMatrix(0, p - 1, 0, t - 1).transpose().times(v)));
			}

			f = f.minusEquals(getHadamardProduct(w, r1.minus(r2)).times(2))
					.plusEquals(r3.times(Math.pow(v.norm2(), 2)));
			f.set(l, 0, 0);
			g = g.minusEquals(r3);
			g.set(l, 0, 0);
			for (int i = 0; i < n; i++) {
				if (f.get(i, 0) < 1e-10)
					f.set(i, 0, 0);

				if (g.get(i, 0) < 1e-10)
					g.set(i, 0, 0);
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
		System.out.println(currMax);
		return (currMax == 0) ? -1 : maxIndex;
	}

	private Matrix getHadamardProduct(Matrix A, Matrix B) {
		// A and B must be of the same size
		int m = A.getRowDimension();
		int n = A.getColumnDimension();
		Matrix result = new Matrix(m, n);
		for (int i = 0; i < m; i++) {
			for (int j = 0; j < n; j++) {
				result.set(i, j, A.get(i, j) * B.get(i, j));
			}
		}
		return result;
	}

	private boolean isExceededRank(Matrix delta, double alphaSqrt) {
		// delta is nx1
		double sumDeltaSqr = 0;
		for (double i : delta.getColumnPackedCopy())
			sumDeltaSqr += i * i;
		return sumDeltaSqr < 1e-40 || alphaSqrt < 1e-40;
	}

	public static void main(String[] args) throws IOException {
		double[][] a = Utilis.loadMatrix("data/A.txt", ",");
		Integer[] cols = new GreedyColSubsetSelection().selectColumnSubset(a,
				a, 10);
		for (int i = 0; i < 10; i++)
			System.out.print(cols[i] + " ");
	}
}