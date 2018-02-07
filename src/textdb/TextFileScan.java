package textdb;

import java.io.*;

/**
 * Performs a file scan in iterator form.  File is assumed to be on local hard drive in TEXT form.  
 */
public class TextFileScan extends Operator
{
	protected String inFileName;					// Name of input file to scan
	protected BufferedReader inFile;				// Used to read from text file
	protected Relation inputRelation;				// Schema of file being scanned


	public TextFileScan(String inName, Relation r)
	{	super();
		inFileName = inName;
		inputRelation = r;
		setOutputRelation(r);						// Set output relation of this operator
	}

	public void init() throws FileNotFoundException, IOException
	{
		// TODO: Assign a new BufferedReader to inFile with the FileManager class
	}

	public Tuple next() throws IOException
	{
		Tuple t = null;

		// TODO: YOUR CODE TO CREATE A NEW TUPLE AND READ FROM TEXT FILE HERE

		incrementTuplesRead();		
		incrementTuplesOutput();
		return t;
	}

	public boolean hasNext() throws IOException
	{	return inFile.ready();
	}

	public void close() throws IOException
	{
		// TODO: YOUR CODE HERE TO CLOSE File using FileManager
	}
}

