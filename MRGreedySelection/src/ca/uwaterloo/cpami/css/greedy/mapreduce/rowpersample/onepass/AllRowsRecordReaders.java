package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample.onepass;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.mahout.math.MatrixWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.VectorWritable;

public class AllRowsRecordReaders extends
		RecordReader<NullWritable, MatrixWritable> {

	private SequenceFile.Reader in;
	private long start;
	private long end;
	private boolean more = true;
	protected Configuration conf;
	private boolean processed = false;
	private MatrixWritable splitRows;

	public AllRowsRecordReaders(Configuration conf, FileSplit split)
			throws IOException {
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		this.in = new SequenceFile.Reader(fs, path, conf);
		this.end = split.getStart() + split.getLength();
		this.conf = conf;
		System.out.println("end: " + end);
		System.out.println("in.pos: " + in.getPosition());
		System.out.println("split start: " + split.getStart());

		if (split.getStart() > in.getPosition())
			in.sync(split.getStart()); // sync to start

		this.start = in.getPosition();
		System.out.println("start after sync: " + this.start);
		more = start < end;
	}

	@Override
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public MatrixWritable getCurrentValue() throws IOException,
			InterruptedException {
		return splitRows;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			// reading all the rows of the split
			IntWritable dummy = new IntWritable();
			VectorWritable tmpVctr = new VectorWritable();
			Map<Integer, RandomAccessSparseVector> rowMap = new HashMap<Integer, RandomAccessSparseVector>();
			Integer numRow = 0, numCols;
			//TODO to be optimized
			while (in.getPosition() < end) {			
				more = in.next(dummy, tmpVctr);				
				if (more) {
					System.out.println(dummy);
					rowMap.put(numRow++,
							new RandomAccessSparseVector(tmpVctr.get()));
				}
			}
			more = in.next(dummy, tmpVctr);				
			while (more&&!in.syncSeen()) {
				System.out.println(dummy);
				rowMap.put(numRow++,
						new RandomAccessSparseVector(tmpVctr.get()));
				more = in.next(dummy, tmpVctr);
			}
			System.out.println("in.pos when done: " + in.getPosition());
			numCols = (numRow == 0) ? 0 : rowMap.get(0).size();
			this.splitRows = new MatrixWritable(new SparseMatrix(numRow,
					numCols, rowMap));
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public void close() throws IOException {
		in.close();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader in = new SequenceFile.Reader(fs, new Path(
				"orth/nips/orth"), conf);

		IntWritable dummy = new IntWritable();
		VectorWritable tmpVctr = new VectorWritable();
		while (in.next(dummy, tmpVctr)) {
			System.out.println(dummy + "\t" + in.getPosition()+"\t"+in.syncSeen());
		}
	}
}
