package textdb;

import java.io.*;
import java.util.ArrayList;

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
	private int finalLeftIdx;
	private int finalRightIdx;
	private int leftPointer;
	private int rightPointer;
	private ArrayList<Tuple> merger;
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


		tupleLeft = input[0].next();
		tupleRight = input[1].next();
		System.out.println("make buffer and check for null:" + checkNull());

	}

	public boolean checkNull() throws IOException{

		if (tupleLeft == null || tupleRight == null) {
			return false;
		} else {
			while (!pred.isEqual(tupleLeft, tupleRight)) {
				if (pred.isLessThan(tupleLeft, tupleRight)) {// check if left less than right
					tupleLeft = input[0].next();// incease left
					if (tupleLeft == null) {
						return false;// returns false if null
					}
				} else {// means left is greater than right 
					tupleRight = input[1].next();//incease right
					if (tupleRight == null) {
						return false; //returns false if null
					}
				}
			}

			if (pred.isEqual(tupleLeft, tupleRight)) { //checks if tuples are equal
				merger = new ArrayList<Tuple>(); //init arraylist
				merger.add(0, tupleLeft); //adds left tuple
				merger.add(1, tupleRight);//adds right tuple
				finalLeftIdx = 1;

				Tuple temp;
				temp = input[0].next();
				if (temp != null) {
					 tupleLeft=temp;

					while (pred.isEqual(tupleLeft, merger.get(0))) {//checks to see if next left tuple is equal to first
						if (finalLeftIdx < MERGE_BUFFER_SIZE) {
							finalLeftIdx++;
							merger.add(finalLeftIdx,tupleLeft);
							tupleLeft = input[0].next();
							if (tupleLeft == null) {
								break;
							}
						}
					}
				}

				temp = input[1].next();
				if (temp != null) {
					tupleRight = temp;
					finalRightIdx = finalLeftIdx;
					while (pred.isEqual(tupleRight, merger.get(1))) {//checks to see if next right is equal to 1st right
						if (finalRightIdx < MERGE_BUFFER_SIZE) {
							finalRightIdx++;
							merger.add(finalRightIdx,tupleRight);
							tupleRight = input[1].next();
							if (tupleRight == null) {
								break;
							}
						}
					}
				}

				
			}
		}
		        leftPointer = 0;
				rightPointer = 1;
				return true;
	}

	public Tuple next() throws IOException {

		//System.out.println(merger.size());
		Tuple temp2 = outputJoinTuple(merger.get(leftPointer), merger.get(rightPointer));
		if (finalLeftIdx == 1 && finalRightIdx == 1) {
			if (!checkNull()) {
				return null;
			} else {
				return temp2;
			}
			// can advance
		} else if (((leftPointer < finalLeftIdx) && (finalLeftIdx > 1))
				|| ((rightPointer < finalRightIdx) && (finalRightIdx > 1))) {
			if (((leftPointer < finalLeftIdx) && (finalLeftIdx > 1))) {
				if (leftPointer < 2) {
					leftPointer = 2;
				} else {
					leftPointer++;
				}
			} else if (((rightPointer < finalRightIdx) && (finalRightIdx > 1))) {
				if (rightPointer <= leftPointer) {
					rightPointer = leftPointer + 1;
				} else {
					rightPointer++;
				}
			}
			return temp2;
		} else if (checkNull()) {
			return temp2;
		}
		return null;
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
