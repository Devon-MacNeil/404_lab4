package junit;


import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;

import textdb.Attribute;
import textdb.BlockNestedLoopJoin;
import textdb.EquiJoinPredicate;
import textdb.Operator;
import textdb.Relation;
import textdb.TextFileScan;

/**
 * Tests block nested loop join implementation.
 */
public class TestNestedBlockJoin {	
    	
	public static String DATA_DIR = "data/";			// Change this if needed to indicate where the data and output directories are.
	public static String OUTPUT_DIR = "output/";
	
	private static Relation r;
	
	@BeforeClass
	public static void init() throws Exception {
		Attribute []attrs = new Attribute[5];

		attrs[0] = new Attribute("key",Attribute.TYPE_INT,0);
		attrs[1] = new Attribute("seq",Attribute.TYPE_INT,0);
		attrs[2] = new Attribute("v1",Attribute.TYPE_INT,0);
		attrs[3] = new Attribute("v2",Attribute.TYPE_INT,0);
		attrs[4] = new Attribute("text",Attribute.TYPE_STRING,100);

		r = new Relation(attrs);
	}	
   
	@Test
    public void testSmallJoin()
	{   System.out.println("\n\nTesting small nested loop join."); 			
		TextFileScan scanLeft = new TextFileScan(DATA_DIR+"smallInputLeft.txt", r);
		TextFileScan scanRight = new TextFileScan(DATA_DIR+"smallInputRight.txt", r);
		EquiJoinPredicate ep = new EquiJoinPredicate(new int[]{0}, new int[]{0}, EquiJoinPredicate.INT_KEY);
		
		BlockNestedLoopJoin bnloop = new BlockNestedLoopJoin(new Operator[]{scanLeft,scanRight}, ep, 1000, 1);		
		
		int count = TestScan.compareOperatorWithOutput(bnloop, OUTPUT_DIR+"nestedBlockOutputSmall.txt");		
		assertEquals(count, 127);
	}
   
	@Test
    public void testLargeJoin()
	{   System.out.println("\n\nTesting large nested loop join."); 							
		TextFileScan scanLeft = new TextFileScan(DATA_DIR+"largeInputLeft.txt", r);
		TextFileScan scanRight = new TextFileScan(DATA_DIR+"largeInputRight.txt", r);
		EquiJoinPredicate ep = new EquiJoinPredicate(new int[]{0}, new int[]{0}, EquiJoinPredicate.INT_KEY);
		
		BlockNestedLoopJoin bnloop = new BlockNestedLoopJoin(new Operator[]{scanLeft,scanRight}, ep, 1000, 1);	
		
		int count = TestScan.compareOperatorWithOutput(bnloop, OUTPUT_DIR+"nestedBlockOutputLarge.txt");		
		assertEquals(count, 191);
	}	
}
