package ca.uwaterloo.cpami.mahout.matrix.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.stochasticsvd.SSVDSolver;

/**
 * 
 * Compute the Pseudo Inverse of a matrix A. A should be given as a
 * DistributedRowMatrix. The computation is based on the stochastic SVD.
 * 
 * @see http://en.wikipedia.org/wiki/Moore-Penrose_pseudoinverse#
 *      Singular_value_decomposition_.28SVD.29
 * 
 */
public class PseudoInverse {

	private static final String TMP_SSVD_OUTPUT_PATH = "/tmp/inv-ssvd-tmp-path";
	private static final int SSVD_P = 15;
	private static final int SSVD_REDUCE_TASKS = 3; // TODO to be tuned
													// depending on the cluster
													// size

	private static final double JVM_EPS = 1.1 * 10e-16;
	private static final String TMP_V_OUT_PATH = "/tmp/v-tmp-out-path";
	private static final String TMP_R_PATH = "/tmp/r-tmp-path";
	private static final String TMP_R_OUT_PATH = "/tmp/r-tmp-out-path";

	public static DistributedRowMatrix invert(Path[] matrixPath, int numRows,
			int numCols) throws IOException {
		cleanTmpDirs();
		int k = Math.min(numRows, numCols);
		int aBlockRows = k + SSVD_P + 100; // TODO find a better way (consider
											// matrix size)
		SSVDSolver ssvdSolver = new SSVDSolver(new Configuration(), matrixPath,
				new Path(TMP_SSVD_OUTPUT_PATH), aBlockRows, k, SSVD_P,
				SSVD_REDUCE_TASKS);
		// to obtain good accuracy
		ssvdSolver.setQ(1);

		ssvdSolver.run();

		Vector sValues = ssvdSolver.getSingularValues();
		Vector sValuesInverted = invertDiagonal(sValues,
				JVM_EPS * Math.max(numRows, numCols) * sValues.get(0));

		DistributedRowMatrix V = new DistributedRowMatrix(new Path(
				ssvdSolver.getUPath()), new Path(TMP_V_OUT_PATH), numCols, k);

		Path rPath = new Path(TMP_R_PATH);
		transposeAndTimesDiagonal(new Path(ssvdSolver.getUPath()),
				sValuesInverted, k, rPath);

		return V.times(new DistributedRowMatrix(rPath,
				new Path(TMP_R_OUT_PATH), k, numRows));
	}

	/**
	 * S is a diagonal matrix of size k x k, the values of the diagonal are
	 * given by the vector v. This method computes a matrix R = S*U'. // TODO to
	 * be executed over a MR job
	 */
	private static void transposeAndTimesDiagonal(Path uPath, Vector v, int k,
			Path outputPath) {

	}

	private static void cleanTmpDirs() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(TMP_SSVD_OUTPUT_PATH), true);
		fs.delete(new Path(TMP_R_OUT_PATH), true);
		fs.delete(new Path(TMP_R_PATH), true);
		fs.delete(new Path(TMP_V_OUT_PATH), true);
	}

	/**
	 * 
	 * Each nonzero element is substituted by its reciprocal. An element is
	 * considered to be nonzero is its value is larger than a certain threshold
	 * given by <i>nonZeroThreshold</i>
	 */
	private static Vector invertDiagonal(Vector v, double nonZeroThreshold) {
		int n = v.size();
		for (int i = 0; i < n; i++) {
			double vi = v.get(i);
			if (vi > nonZeroThreshold)
				v.set(i, 1 / vi);
		}
		return null;
	}

}
