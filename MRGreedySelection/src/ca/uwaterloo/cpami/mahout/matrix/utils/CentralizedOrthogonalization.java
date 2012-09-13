package ca.uwaterloo.cpami.mahout.matrix.utils;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.hadoop.stochasticsvd.qr.GramSchmidt;

import ca.uwaterloo.cpami.css.dataprep.SequenceFileToCSV;

/**
 * 
 * loads a matrix from a sequence file, apply Gram-Schmidt, write orthonormal
 * matrix as a sequence file
 * 
 */
public class CentralizedOrthogonalization {

	public static void orthonormalize(Path orignalMatrixPath, int numRows,
			int numCols, Path orthonormalMatrixPath) throws IOException {
		Matrix m = Helpers.loadMatrix(orignalMatrixPath, numRows, numCols);
		GramSchmidt.orthonormalizeColumns(m);
		Helpers.writeMatrix(m, orthonormalMatrixPath);
	}

	// test
	public static void main(String[] args) throws IOException {

		CentralizedOrthogonalization.orthonormalize(new Path("/lanc/BP22"), 100,
				100, new Path("/orth/orthY/Y"));
		SequenceFileToCSV.sequenceFileToCSV("/orth/orthY", "/orth/Yorth.csv",
				",");
	}
}
