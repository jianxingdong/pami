package ca.uwaterloo.cpami.css.baselines;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;

import ca.uwaterloo.cpami.mahout.matrix.utils.FrobeniusNormDiffJob;
import ca.uwaterloo.cpami.mahout.matrix.utils.PseudoInverse;

/**
 * 
 * Given two matrices A (the original matrix) and C (a matrix to project to its
 * span), the reconstruction error defined as ||A-C*(C'*C)^-1*C'*A||_F is
 * computed. The two matrices should be given as DistributedRowMatrix
 * 
 */
public class ReconstructionError {

	/**
	 * 
	 * @param A
	 * @param C
	 * @param numCColumns
	 *            : the leading numCColumns of will be only be considered
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * 
	 */
	public double clacReconstructionErr(DistributedRowMatrix A,
			DistributedRowMatrix C, int numCColumns)
			throws IllegalArgumentException, IOException, InterruptedException,
			ClassNotFoundException {
		PseudoInverse pseudoInverse = new PseudoInverse();
		// for now 1
		pseudoInverse.setNumReducers(1);
		DistributedRowMatrix cT = C.transpose();
		DistributedRowMatrix B = cT
				.times(pseudoInverse.invert(new Path[] { C.times(cT)
						.transpose().getRowPath() }, C.numCols(), C.numCols()))
				.transpose().times(C.times(A));

		return new FrobeniusNormDiffJob().calcFrobeniusNorm(A.getRowPath(),
				B.getRowPath());
	}

	// test
	public static void main(String[] args) throws IOException,
			IllegalArgumentException, InterruptedException,
			ClassNotFoundException {
		Configuration conf = new Configuration();
		DistributedRowMatrix A = new DistributedRowMatrix(new Path("/inv/U"),
				new Path("/tmp1/A"), 99, 99);
		A.setConf(conf);
		DistributedRowMatrix C = new DistributedRowMatrix(new Path("/inv/U"),
				new Path("/tmp1/B"), 99, 99);
		C.setConf(conf);
		System.out.println(new ReconstructionError().clacReconstructionErr(A,
				C, 99));

	}
}
