/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ca.uwaterloo.cpami.mahout.matrix.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.SparseRealMatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleFunction;

/**
 * Gram Schmidt quick helper.
 * 
 * 
 */
public class GramSchmidt {

	private GramSchmidt() {
	}

	public static void orthonormalizeColumns(Matrix mx) {

		int n = mx.numCols();

		for (int c = 0; c < n; c++) {
			System.out.println("col: " + c);
			Vector col = mx.viewColumn(c);
			for (int c1 = 0; c1 < c; c1++) {
				Vector viewC1 = mx.viewColumn(c1);
				col.assign(col.minus(viewC1.times(viewC1.dot(col))));

			}
			final double norm2 = col.norm(2);
			if(norm2==0){
				System.out.println("zero");
			}
			col.assign(new DoubleFunction() {
				@Override
				public double apply(double x) {
					return x / norm2;
				}
			});
		}
	}

	public static void main(String[] args) throws IOException {		
	
		RandomAccessSparseVector v = new RandomAccessSparseVector(3);
		v.set(0, 1); 		v.set(1, 2);  		v.set(2, 3);
		System.out.println(v.norm(2)*v.norm(2));
		System.exit(1);
		//final Configuration conf = new Configuration();
		//final FileSystem fs = FileSystem.get(conf);
		//final SequenceFile.Reader reader = new SequenceFile.Reader(fs,
			//	new Path("R1.dat"), conf);
		//IntWritable key = new IntWritable();
		//VectorWritable vec = new VectorWritable();
		Matrix mat = new SparseMatrix(1500, 100);
		//SparseRealMatrix mat2 = new OpenMapRealMatrix(12419,1500 );
		BufferedReader reader = new BufferedReader(new FileReader("R.csv"));
		String line = null;
		while ((line=reader.readLine())!=null) {		
		String[] parts = line.split(",");
		mat.set(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Double.parseDouble(parts[2]));
			/*
			Vector v = vec.get();
			int i=0;
			Iterator<Vector.Element> itr = v.iterateNonZero();
			while(itr.hasNext()){
				double elem = itr.next().get();
				if(elem !=0)
					mat2.setEntry(i, key.get(), elem);
				i++;
			}
			*/						
		}
		
		//mat = mat.transpose();
		System.out.println(mat.viewColumn(0).isDense());
		System.out.println(mat.viewRow(0).isDense());
		GramSchmidt.orthonormalizeColumns(mat);
		/*
		System.out.println("started QR");
		System.out.println(Runtime.getRuntime().maxMemory());
		System.out.println(Runtime.getRuntime().maxMemory()-Runtime.getRuntime().freeMemory());
		QRDecomposition qr = new QRDecomposition(mat2);
		System.out.println(qr.getQ().getColumnDimension());
		System.out.println(qr.getQ().getRowDimension());
		*/
		//mat = mat.transpose();
		//storeSparseColumns(mat);
		//for (int i = 0; i < 10; i++) {
		//	System.out.println(mat.viewRow(i).getNumNondefaultElements());
		//}
		
		
	}

	public static void storeSparseColumns(Matrix mat) {
		int numCols = mat.numCols();
		int numRows = mat.numRows();
		for (int i = 0; i < numCols; i++) {			
			Vector sparseVect = new RandomAccessSparseVector(numRows);
			Vector col = mat.viewColumn(i);
			Iterator<Vector.Element> itr = col.iterateNonZero();
			while (itr.hasNext()) {
				Element elem = itr.next();
				if (elem.get() != 0) {
					System.out.println(elem.get());
					sparseVect.set(elem.index(), elem.get());
				}
			}
			System.out.println(sparseVect.getNumNondefaultElements());
			
			mat.assignColumn(i, sparseVect);
			System.out.println(mat.viewColumn(i).getNumNondefaultElements());
			System.exit(1);
			
		}
	}

}
