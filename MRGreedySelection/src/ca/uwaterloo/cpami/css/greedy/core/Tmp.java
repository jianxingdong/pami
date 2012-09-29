package ca.uwaterloo.cpami.css.greedy.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;

public class Tmp {

	/**
	 * 
	 * @param k
	 *            num cols to select
	 * @return the indices of the selected columns (indicies start from 0)
	 * @throws Exception
	 */
	public Integer[] selectColumnSubset(Matrix sourceMat, Matrix targetMat,
			int k) {

		Matrix tt = targetMat.transpose();
		System.out.println("tt done");
		// Matrix G = tt.times(sourceMat); // pxn
		Matrix G = Utilis.multiply(tt, sourceMat); // pxn
		int p = G.numRows();
		int n = G.numCols();
		int m = sourceMat.numRows();
		System.out.println("G done");
		Vector f = new DenseVector(n); // nx1
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < p; j++) {
				f.set(i, f.get(i) + G.get(i, j) * G.get(i, j));
			}
		}
		System.out.println("f done");
		Vector g = new DenseVector(n); // nx1

		for (int i = 0; i < n; i++) {
			for (int j = 0; j < m; j++) {
				g.set(i, g.get(i) + sourceMat.get(j, i) * sourceMat.get(j, i));
			}
		}
		System.out.println("g done");
		ArrayList<Integer> selectedIndices = new ArrayList<Integer>(k);
		HashSet<Integer> selectedIndicesHash = new HashSet<Integer>();
		Matrix W = new DenseMatrix(n, k); // nxk
		Matrix V = new DenseMatrix(p, k); // pxk

		for (int t = 0; t < k; t++) {
			int l = findMaxRatio(f, g, n, selectedIndicesHash);
			if (l == -1) {
				System.out.println("l == -1");
				break;
			}
			System.out.println(l);
			selectedIndices.add(l);
			selectedIndicesHash.add(l);
			// modify f and g

			Vector delta = sourceMat.transpose().times(sourceMat.viewColumn(l)); // nx1

			Vector gamma = getColumns(G, l); // px1

			if (t > 0) {
				delta = delta.minus(getColumns(W.viewPart(0, n - 1, 0, t - 1)
						.times(W.viewPart(l, l, 0, t - 1).transpose()), 0));

				gamma = gamma.minus(getColumns(W.viewPart(0, p - 1, 0, t - 1)
						.times(W.viewPart(l, l, 0, t - 1).transpose()), 0));
			}

			double alphaSqrt = Math.sqrt(delta.get(l));
			Vector w = delta.times(1.0 / alphaSqrt); // nx1

			setColumn(W, t, w);

			Vector v = gamma.times(1.0 / alphaSqrt); // px1
			setColumn(V, t, v);

			if (isExceededRank(delta, alphaSqrt)) {
				System.out.println("excceded the rank");
				break;
			}
			System.out.println("total: " + Runtime.getRuntime().totalMemory());
			System.out.println("free: " + Runtime.getRuntime().freeMemory());
			Vector r1 = G.transpose().times(v); // nx1

			Vector r3 = getHadamardProduct(w, w); // nx1

			Vector r2 = new DenseVector(n);
			if (t > 0) {
				r2 = W.viewPart(0, n - 1, 0, t - 1).times(
						V.viewPart(0, p - 1, 0, t - 1).transpose().times(v));
			}

			f = f.minus(getHadamardProduct(w, r1.minus(r2)).times(2).plus(
					r3.times(Math.pow(v.norm(2), 2))));
			f.set(l, 0);
			g = g.minus(r3);
			g.set(l, 0);
			for (int i = 0; i < n; i++) {
				if (f.get(i) < 1e-10)
					f.set(i, 0);

				if (g.get(i) < 1e-10)
					g.set(i, 0);
			}
		}

		return selectedIndices.toArray(new Integer[] {});
	}

	private Vector getColumns(Matrix m, int col) {
		int numRows = m.numRows();
		Vector v = (m instanceof DenseMatrix) ? new DenseVector()
				: new RandomAccessSparseVector(numRows);
		for (int i = 0; i < numRows; i++) {
			if (m.get(i, col) != 0)
				v.set(i, m.get(i, col));
		}
		return v;
	}

	private void setColumn(Matrix m, int colId, Vector col) {
		int numRows = col.size();
		for (int i = 0; i < numRows; i++) {
			m.set(i, colId, col.get(i));
		}
	}

	/**
	 * 
	 * 
	 * @return -1 if maxScore = 0
	 */
	private int findMaxRatio(Vector f, Vector g, int n,
			HashSet<Integer> selectedBefore) {
		double currMax = Double.MIN_VALUE;
		int maxIndex = -1;
		for (int i = 0; i < n; i++) {
			if (selectedBefore.contains(i))
				continue;
			double r = f.get(i) / g.get(i);
			if (Double.isInfinite(r) || Double.isNaN(r))
				continue;
			if (r > currMax) {
				currMax = r;
				maxIndex = i;
			}
		}
		return (currMax == 0) ? -1 : maxIndex;
	}

	private Vector getHadamardProduct(Vector v1, Vector v2) {

		// A and B must be of the same size
		int m = v1.size();
		Vector result = (v1 instanceof DenseVector && v2 instanceof DenseVector) ? new DenseVector(
				m) : new RandomAccessSparseVector(m);

		for (int i = 0; i < m; i++) {
			result.set(i, v1.get(i) * v2.get(i));
		}
		return result;
	}

	private boolean isExceededRank(Vector delta, double alphaSqrt) {
		// delta is nx1
		double sumDeltaSqr = Math.pow(delta.norm(2), 2);
		return sumDeltaSqr < 1e-40 || alphaSqrt < 1e-40;
	}

	public static void main(String[] args) throws IOException {
		SparseMatrix mat = Utilis.loadSparseMatrix(
				"/home/ahmed/Desktop/Thesis/ICDM13/dataset/kos-full.txt", ",",
				6906, 3430);
		System.gc();
		System.gc();
		System.out.println("A Size:"
				+ (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
						.freeMemory()) / (1024 * 1024));
		Matrix B = Utilis.partition(mat, 1000);
		System.gc();
		System.gc();
		System.out.println("A +B Size:"
				+ (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
						.freeMemory()) / (1024 * 1024));
		long time = System.currentTimeMillis();
		Matrix G = Utilis.transposeTimes(B, mat);
		System.gc();
		System.gc();
		System.out.println("A +B + G Size:"
				+ (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
						.freeMemory()) / (1024 * 1024));
		System.out.println(System.currentTimeMillis() - time);
		System.out.println(G.viewRow(0).getNumNondefaultElements());
		System.out.println(G.viewRow(10).getNumNondefaultElements());
		System.exit(1);
		SparseMatrix mat2 = (SparseMatrix) mat.transpose();
		long t = System.currentTimeMillis();
		mat.times(mat2);
		System.out.println("time: " + (System.currentTimeMillis() - t));
		System.exit(1);
		System.out.println("start");
		Integer[] cols = new Tmp().selectColumnSubset(mat, mat.clone(), 100);
		for (int i = 0; i < 100; i++)
			System.out.print(cols[i] + " ");
	}
}