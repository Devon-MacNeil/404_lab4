package textdb;

import java.io.*;

/**
 * Contains code for performing a two-pass external merge join in iterator
 * format.
 */
public class MergeJoin extends Operator {
	private int MERGE_BUFFER_SIZE = 10000; // The number of tuples that can be
											// buffered with the same key.
	private EquiJoinPredicate pred; // A equi-join comparison class that can
									// handle 1 or more attributes
	private Tuple tupleLeft;
	private Tuple tupleRight;
	private Tuple smallestTuple;
	private Tuple temp;
	private Tuple[] buffArry;
	// Iterator state variables

	public MergeJoin(Operator[] in, EquiJoinPredicate p) {
		super(in, 0, 0);
		pred = p;
	}

	public void init() throws IOException {
		input[0].init();
		input[1].init();

		// Create output relation - keep all attributes of both tuples
		Relation out = new Relation(input[0].getOutputRelation());
		out.mergeRelation(input[1].getOutputRelation());
		setOutputRelation(out);

		// TODO: YOUR SETUP CODE HERE
		buffArry = new Tuple[MERGE_BUFFER_SIZE];
		   tupleLeft = input[0].next();
		   tupleRight = input[1].next();
		

	}

	public Tuple next() throws IOException {
		if(tupleLeft == null || tupleRight == null){
			return null;
		}else{
		
		while(!pred.isEqual(tupleLeft, tupleRight)){
			if(pred.isLessThan(tupleLeft, tupleRight)){//check if left is less than right
				tupleLeft = input[0].next();//inc left
			}else{//means left is greater than right
				tupleRight = input[1].next();//inc right
			}
		}
		  if(pred.isEqual(tupleLeft, tupleRight)){
				temp = outputJoinTuple(tupleLeft, tupleRight);
				tupleLeft = input[0].next();
				tupleRight = input[1].next();
		  }
		return temp;
		}
		
		
	}

	public void close() throws IOException {
		super.close();
	}

	private Tuple outputJoinTuple(Tuple left, Tuple right) {
		Tuple t = new Tuple(left, right, getOutputRelation());
		incrementTuplesOutput();
		return t;
	}
}
