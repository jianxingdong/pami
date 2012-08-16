package ca.uwaterloo.cpami.css.baselines;

import org.apache.mahout.math.MatrixUtils;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;

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
	 * 
	 */
	public static double clacReconstructionErr(DistributedRowMatrix A,
			DistributedRowMatrix C, int numCColumns)
			throws IllegalArgumentException {
		if (A.numRows() != C.numRows()) {
			throw new IllegalArgumentException("A.numRows != C.numRows");
		}
		
		
		return 0;
	}
}
