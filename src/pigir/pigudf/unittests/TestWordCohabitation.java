package pigir.pigudf.unittests;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import pigir.Common;
import pigir.pigudf.MakeWordPairDistances;

public class TestWordCohabitation {
	
	
	public TestWordCohabitation() {
		
	}
	
	class Truth {
		public String word1;
		public String word2;
		public int distance;
		public String docID;
		
		public Truth(String theWord1, String theWord2, int theDistance, String theDocID) {
			word1 = theWord1;
			word2 = theWord2;
			distance = theDistance;
			docID = theDocID;
		}
		public String toString() {
			return "Truth[(" + word1 + "," + word2 + "," + distance + "," + docID + ")]";
		}
	}
	
	
	/**
	 * For each result from MakeWordPairDistance, verify that every tuple is correct.
	 * 
	 * @param result is a tuple: ((word1,word2,distance,docID), (word1,word2,distance,docID),...). All docID identical
	 * @param groundTruth an array of Truth objects. The objects are ordered as in the expected result.
	 * @return true/false.
	 */
	private static boolean matchOutput(Tuple result, ArrayList<Truth> groundTruth) {

 		Iterator<Object> resultIt = Common.getTupleIterator(result);
		Iterator<Truth> truthIt   = groundTruth.iterator();
		Tuple nextRes = null;
		Truth nextTruth = null;

		try {

			if (result.size() == 0 && groundTruth.size() == 0)
				return true;
			
			while (resultIt.hasNext()) {
				if (! truthIt.hasNext())
					return false;
				nextRes   = (Tuple) resultIt.next();
				nextTruth = truthIt.next();
				if (!nextRes.get(0).equals(nextTruth.word1) || 
					!nextRes.get(1).equals(nextTruth.word2) || 
					!nextRes.get(2).equals(nextTruth.distance) ||
					!nextRes.get(3).equals(nextTruth.docID))
					return false;
			}
			if (truthIt.hasNext())
				return false;

			return true;
		} catch (ExecException e) {
			e.printStackTrace();
		}
		return true;
	}
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws ExecException {
		
	/*
 	 * 	First call's input : ({(man,d1,0),(lives,d1,1),(sun,d1,2)})
	 *	Second call's input: ({(Girl,d2,0),(child,d2,1),(lives,d2,2),(close,d2,3)})
		
     *    (man,lives,1,d1)
     *    (man,sun,2,d1)
     *    (lives,sun,1,d1)
     */
		
		MakeWordPairDistances func = new MakeWordPairDistances();

		final TestWordCohabitation tester = new TestWordCohabitation();
		
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parm = tupleFac.newTuple();
		DataBag inBag = BagFactory.getInstance().newDefaultBag();
		parm.append(inBag);
		
		// First, test fastLowerCaseWord():
		func.setWord("Girl");
		func.fastLowerCaseWord();
		assertTrue(func.getWord().equals("girl"));
		func.setWord("girl");
		func.fastLowerCaseWord();
		assertTrue(func.getWord().equals("girl"));
		func.setWord("$Girl");
		func.fastLowerCaseWord();
		assertTrue(func.getWord().equals("$Girl"));
		
		try {
			
			// For an index build from the document "This man lives in the sun."
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("man");
					add("d1");
					add(0);
				}
			}));
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("lives");
					add("d1");
					add(1);
				}
			}));
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("sun");
					add("d1");
					add(2);
				}
			}));
			
			assertTrue(matchOutput(func.exec(parm), new ArrayList<Truth>() {
				{
					add(tester.new Truth("man", "lives", 1, "d1"));
					add(tester.new Truth("lives", "sun", 1, "d1"));
					add(tester.new Truth("man", "sun", 2, "d1"));
				};
			})); 
			
			inBag.clear();
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("Girl");
					add("d2");
					add(0);
				}
			}));
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("child");
					add("d2");
					add(1);
				}
			}));
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("lives");
					add("d2");
					add(2);
				}
			}));
			
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("close");
					add("d2");
					add(3);
				}
			}));
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("wife");
					add("d2");
					add(4);
				}
			}));
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("near");
					add("d2");
					add(5);
				}
			}));
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("me");
					add("d2");
					add(6);
				}
			}));
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("lives");
					add("d2");
					add(7);
				}
			}));
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("station");
					add("d2");
					add(8);
				}
			}));
			inBag.add(tupleFac.newTuple(new ArrayList<Object>() {
				{
					add("train");
					add("d2");
					add(9);
				}
			}));
			assertTrue(matchOutput(func.exec(parm), new ArrayList<Truth>() {
				{
					add(tester.new Truth("girl", "child", 1, "d2"));
					add(tester.new Truth("child", "lives", 1, "d2"));
					add(tester.new Truth("girl", "lives", 2, "d2"));
					add(tester.new Truth("lives", "close", 1, "d2"));
					add(tester.new Truth("child", "close", 2, "d2"));
					add(tester.new Truth("girl", "close", 3, "d2"));
					add(tester.new Truth("close", "wife", 1, "d2"));
					add(tester.new Truth("lives", "wife", 2, "d2"));
					add(tester.new Truth("child", "wife", 3, "d2"));
					add(tester.new Truth("girl", "wife", 4, "d2"));
					add(tester.new Truth("wife", "near", 1, "d2"));
					add(tester.new Truth("close", "near", 2, "d2"));
					add(tester.new Truth("lives", "near", 3, "d2"));
					add(tester.new Truth("child", "near", 4, "d2"));
					add(tester.new Truth("girl", "near", 5, "d2"));
					add(tester.new Truth("near", "me", 1, "d2"));
					add(tester.new Truth("wife", "me", 2, "d2"));
					add(tester.new Truth("close", "me", 3, "d2"));
					add(tester.new Truth("lives", "me", 4, "d2"));
					add(tester.new Truth("child", "me", 5, "d2"));
					//add(tester.new Truth("girl", "me", 6, "d2"));
					add(tester.new Truth("me", "lives", 1, "d2"));
					add(tester.new Truth("near", "lives", 2, "d2"));
					add(tester.new Truth("wife", "lives", 3, "d2"));
					add(tester.new Truth("close", "lives", 4, "d2"));
					add(tester.new Truth("lives", "lives", 5, "d2"));
					//add(tester.new Truth("child", "lives", 6, "d2"));
					//add(tester.new Truth("girl", "lives", 7, "d2"));
					add(tester.new Truth("lives", "station", 1, "d2"));
					add(tester.new Truth("me", "station", 2, "d2"));
					add(tester.new Truth("near", "station", 3, "d2"));
					add(tester.new Truth("wife", "station", 4, "d2"));
					add(tester.new Truth("close", "station", 5, "d2"));
					//add(tester.new Truth("lives", "station", 6, "d2"));
					//add(tester.new Truth("child", "station", 7, "d2"));
					//add(tester.new Truth("girl", "station", 8, "d2"));
					add(tester.new Truth("station", "train", 1, "d2"));
					add(tester.new Truth("lives", "train", 2, "d2"));
					add(tester.new Truth("me", "train", 3, "d2"));
					add(tester.new Truth("near", "train", 4, "d2"));
					add(tester.new Truth("wife", "train", 5, "d2"));
					//add(tester.new Truth("close", "train", 6, "d2"));
					//add(tester.new Truth("lives", "train", 7, "d2"));
					//add(tester.new Truth("child", "train", 8, "d2"));
					//add(tester.new Truth("girl", "train", 9, "d2"));
				};
			})); 
			
			
			System.out.println("Output schema: " + func.outputSchema(null));
			System.out.println("All tests passed.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
