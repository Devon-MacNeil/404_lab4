package textdb;


import java.io.IOException;

/**
 * Implements block nested loop join.
 */
public class BlockNestedLoopJoin extends Operator
{
	

	public BlockNestedLoopJoin(Operator []in, EquiJoinPredicate p, int bsize, int bfr)
	{	super(in, bfr, bsize);		
	}

	public void init() throws IOException
	{
		// TODO
	}


	public Tuple next() throws IOException
	{
		// TODO
		return null;
	}

	public void close() throws IOException
	{	
		// TODO
	}
}

