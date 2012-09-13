package ca.uwaterloo.cpami.css.baselines;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;

import ca.uwaterloo.cpami.mahout.matrix.utils.FrobeniusNormDiffJob;
import ca.uwaterloo.cpami.mahout.matrix.utils.Helpers;

/**
 * 
 * Given two matrices A (the original matrix) and C (a matrix to project to its
 * span), the reconstruction error defined as ||A-C*(C'*C)^-1*C'*A||_F is
 * computed. The two matrices should be given as DistributedRowMatrix
 * 
 * TODO for now C must be orthogonal/semi-orthogonal
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
			DistributedRowMatrix C, int numReducers)
			throws IllegalArgumentException, IOException, InterruptedException,
			ClassNotFoundException {
		/*
		 * PseudoInverse pseudoInverse = new PseudoInverse();
		 * pseudoInverse.setNumReducers(numReducers); DistributedRowMatrix cT =
		 * C.transpose(); DistributedRowMatrix cTc = C.times(C);
		 * DistributedRowMatrix cTcInv = pseudoInverse.invert( new Path[] {
		 * cTc.getRowPath() }, cTc.numRows(), cTc.numCols());
		 * DistributedRowMatrix c_ctcInv = cT.times(cTcInv);
		 * DistributedRowMatrix c_ctcInv_t = c_ctcInv.transpose();
		 * DistributedRowMatrix cT_A = C.times(A); DistributedRowMatrix B =
		 * c_ctcInv_t.times(cT_A);
		 */

		DistributedRowMatrix CTA = multiply(A, C);
		DistributedRowMatrix CT = C.transpose();
		DistributedRowMatrix CCTA = multiply(CTA, CT);
		return new FrobeniusNormDiffJob().calcFrobeniusNorm(A.getRowPath(),
				CCTA.getRowPath(), numReducers);
	}

	private DistributedRowMatrix multiply(DistributedRowMatrix A,
			DistributedRowMatrix C) throws IOException, InterruptedException,
			ClassNotFoundException {
		int CNumParts = Helpers.getNumPrtitions(C.getRowPath());
		int ANumParts = Helpers.getNumPrtitions(A.getRowPath());
		if (CNumParts == ANumParts) {
			return C.times(A);
		} else {
			// C is smaller, repartition it
			Path newCPath = new Path(C.getRowPath().getParent(), C.getRowPath()
					.getName() + "-repartition");
			Helpers.repartitionMatrix(C.getRowPath(), newCPath, ANumParts);
			DistributedRowMatrix CP = new DistributedRowMatrix(newCPath,
					C.getOutputTempPath(), C.numRows(), C.numCols());
			CP.setConf(new Configuration());
			return CP.times(A);
		}
	}
}