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
 *      TODO: take a tmp directory as a parameter. TODO: compare to
 *      Lanczos-based computation
 */
public class PseudoInverse {

	private static final String TMP_SSVD_OUTPUT_PATH = "/tmp/5/inv-ssvd-tmp-path";

	private static final double JVM_EPS = 1.1e-16;
	private static final String TMP_V_OUT_PATH = "/tmpinv/4/v-tmp-out-path";
	private static final String TMP_R_PATH = "/tmpinv/3/r-tmp-path";
	private static final String TMP_R_OUT_PATH = "/tmpinv/2/r-tmp-out-path";
	private static final String TMP_TTD_PATH = "/tmpinv/1/ttd";

	// when applicable
	private int numReducers = 1;

	/**
	 * 
	 * @param numReducers
	 *            : when applicable
	 */
	public void setNumReducers(int numReducers) {
		// this.numReducers = numReducers;
		// TODO
		this.numReducers = 1;
	}

	public DistributedRowMatrix invert(Path[] matrixPath, int numRows,
			int numCols) throws IOException {
		cleanTmpDirs();
		Configuration conf = new Configuration();
		// ssvd assumes that m >= k+p and n>=k+p
		int k = (int) (0.5 * Math.min(numRows, numCols));
		int oversampling = Math.max(2, (int) (0.1 * k));

		// TODO find a better way (consider matrix size)
		int aBlockRows = 10000;
		SSVDSolver ssvdSolver = new SSVDSolver(conf, matrixPath, new Path(
				TMP_SSVD_OUTPUT_PATH), aBlockRows, k, oversampling, numReducers);
		// for a good accuracy
		ssvdSolver.setQ(1);

		ssvdSolver.run();

		Vector sValues = ssvdSolver.getSingularValues();
		Vector sValuesInverted = invertDiagonal(sValues,
				JVM_EPS * Math.max(numRows, numCols) * sValues.get(0));

		Path rPath = new Path(TMP_R_PATH);
		transposeAndTimesDiagonal(new Path(ssvdSolver.getUPath()), numRows, k,
				sValuesInverted, TMP_R_PATH);

		DistributedRowMatrix V = new DistributedRowMatrix(new Path(
				ssvdSolver.getVPath()), new Path(TMP_V_OUT_PATH), numCols, k);
		V.setConf(conf);

		DistributedRowMatrix R = new DistributedRowMatrix(rPath, new Path(
				TMP_R_OUT_PATH), k, numRows);
		R.setConf(conf);

		DistributedRowMatrix result = V.transpose().times(R);
		return result;
	}

	/**
	 * S is a diagonal matrix of size numCols x numCols, the values of the
	 * diagonal are given by the vector v. This method computes a matrix R =
	 * S*U'.
	 */
	private void transposeAndTimesDiagonal(Path uPath, int numRows,
			int numCols, Vector v, String outputPath) {
		try {
			// have to use a single reducers -> CompositeInputFormat (R and V
			// have to have the same splits)
			new TransposeAndTimesDiagonal()
					.run(v, TMP_TTD_PATH, uPath.toString(), numRows, numCols,
							outputPath, numReducers);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("TransposeAndTimesDiagonal Failed");
		}
	}

	private static void cleanTmpDirs() throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(TMP_SSVD_OUTPUT_PATH), true);
		fs.delete(new Path(TMP_R_OUT_PATH), true);
		fs.delete(new Path(TMP_R_PATH), true);
		fs.delete(new Path(TMP_V_OUT_PATH), true);
		fs.delete(new Path(TMP_TTD_PATH), true);
	}

	/**
	 * 
	 * Each nonzero element is substituted by its reciprocal. An element is
	 * considered nonzero if its value is larger than a certain threshold given
	 * by <i>nonZeroThreshold</i>
	 */
	private static Vector invertDiagonal(Vector v, double nonZeroThreshold) {
		int n = v.size();
		for (int i = 0; i < n; i++) {
			double vi = v.get(i);
			if (vi > nonZeroThreshold)
				v.set(i, 1 / vi);
			else
				v.set(i, 0);
		}
		return v;
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		DistributedRowMatrix vt = new DistributedRowMatrix(new Path(
				"/tmp/5/inv-ssvd-tmp-path/transpose-46"),
				new Path("/d/tmp/vt"), 15, 30);
		vt.setConf(conf);
		DistributedRowMatrix R = new DistributedRowMatrix(new Path(
				"/tmpinv/3/r-tmp-path"), new Path("/d/tmp/r"), 15, 30);
		R.setConf(conf);

		DistributedRowMatrix drm = vt.times(R);
		System.out.println(drm.getRowPath());
	}

}
