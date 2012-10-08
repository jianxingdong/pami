package ca.uwaterloo.cpami.css.greedy.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;

//memory needed = mxnxs1 + nxnxs2
//s1 -> sparsity of the original matrix
//s2 -> sparsity of A'*A >> s1 (maybe = 1 i.e. A'*A is closer to dense)
public class LocalGreedyCSS {

	// to do random partitioning, take another matrix target
	public Integer[] selectColumnSubset(Matrix sourceMatTranspose, int k) {

		Matrix GTranspose = Utilis.calcATimeATranspose(sourceMatTranspose); // nxp
		int n = GTranspose.numRows();
		int p = GTranspose.numCols();
		int m = sourceMatTranspose.numCols();
		Vector f = new DenseVector(n); // nx1
		for (int i = 0; i < n; i++) {
			for (int j = 0; j < p; j++) {
				f.set(i, f.get(i) + GTranspose.get(j, i) * GTranspose.get(j, i));
			}
		}
		Vector g = new DenseVector(n); // nx1

		for (int i = 0; i < n; i++) {
			for (int j = 0; j < m; j++) {
				g.set(i, g.get(i) + sourceMatTranspose.get(i, j)
						* sourceMatTranspose.get(i, j));
			}
		}
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
			selectedIndices.add(l);
			selectedIndicesHash.add(l);
			// modify f and g
			Vector delta = sourceMatTranspose.times(sourceMatTranspose
					.viewRow(l)); // nx1

			Vector gamma = GTranspose.viewRow(l); // px1
			// getColumns(G, l); // px1

			if (t > 0) {
				delta = delta.minus(W.viewPart(0, n, 0, t).times(
						W.viewRow(l).viewPart(0, t)));

				gamma = gamma.minus(V.viewPart(0, p, 0, t).times(
						W.viewRow(l).viewPart(0, t)));

			}

			double alphaSqrt = Math.sqrt(delta.get(l));
			Vector w = delta.times(1.0 / alphaSqrt); // nx1
			W.assignColumn(t, w);

			Vector v = gamma.times(1.0 / alphaSqrt); // px1
			V.assignColumn(t, v);

			if (isExceededRank(delta, alphaSqrt)) {
				System.out.println("excceded the rank");
				break;
			}

			Vector r1 = GTranspose.times(v); // nx1

			Vector r3 = getHadamardProduct(w, w); // nx1

			Vector r2 = new DenseVector(n);
			if (t > 0) {
				r2 = W.viewPart(0, n, 0, t).times(
						V.viewPart(0, p, 0, t).transpose().times(v));
			}
			f = f.minus(getHadamardProduct(w, r1.minus(r2)).times(2)).plus(
					r3.times(Math.pow(v.norm(2), 2)));
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
		Vector result = v1.like();
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
		Matrix mx = mat.viewPart(0, 100, 0, 3430);
		mx = mx.transpose();
		Integer[] cols = new LocalGreedyCSS().selectColumnSubset(mx, 10);
		for (int i = 0; i < 10; i++)
			System.out.print(cols[i] + " ");
	}
}