package pigir.pigudf.unittests;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import pigir.Common;
import pigir.pigudf.PartOfSpeechTag;


public class TestPartOfSpeechTag {

	private static boolean matchOutput(Tuple result, ArrayList<GroundTruth> groundTruth) {

		Iterator<Object> resultIt = Common.getTupleIterator(result);
		Iterator<GroundTruth> truthIt  = groundTruth.iterator();
		Tuple nextRes = null;
		GroundTruth nextTruth = null;

		if (result.size() == 0 && groundTruth.size() == 0)
			return true;
		
		try {
		while (resultIt.hasNext()) {
			if (! truthIt.hasNext())
				return false;
			nextRes   = (Tuple) resultIt.next();
			nextTruth = truthIt.next();
			if (!nextRes.get(0).equals(nextTruth.word) ||
				!nextRes.get(1).equals(nextTruth.tag))
				return false;
		}
		if (truthIt.hasNext())
			return false;
		} catch (Exception e) {
			return false;
		}
		
		return true;
	}
	
	private static class GroundTruth {
		public String word;
		public String tag;
		
		public GroundTruth(String theWord, String theTag) {
			word = theWord;
			tag  = theTag;
		}
		public String toString() {
			return "Truth<" + word + "," + tag + ">";
		}
	}
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws ExecException {
		
		PartOfSpeechTag func = null;
		Tuple result;
		
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		
		try {
			// No HTML tags:
			func = new PartOfSpeechTag();
			parms.set(0, "My Bonnie lies over the ocean.");
			result = func.exec(parms);
			assertTrue(matchOutput(result, new ArrayList<GroundTruth>() {
				{
					add(new GroundTruth("My", "PRP$"));
					add(new GroundTruth("Bonnie","NNP"));
					add(new GroundTruth("lies","VBZ"));
					add(new GroundTruth("over","IN"));
					add(new GroundTruth("ocean","NN"));
				};
			}));
			
			// Empty string:
			parms.set(0, "");
			assertNull(result = func.exec(parms));
			
			// HTML tag <p> when only content within <p> is wanted:
			func = new PartOfSpeechTag("p");
			parms.set(0, "My Bonnie <p>lies over</p> the ocean.");
			result = func.exec(parms);
			assertTrue(matchOutput(result, new ArrayList<GroundTruth>() {
				{
					add(new GroundTruth("lies", "NNS"));
					add(new GroundTruth("over","IN"));
				};
			}));
			
			// HTML tags <p> and <b> not nested: 
			func = new PartOfSpeechTag("p b");
			parms.set(0, "My Bonnie <p>lies over</p> the <b>ocean</b>.");
			result = func.exec(parms);
			assertNull(result);
			
			// HTML tags <p> and <b> with <b> nested inside of <p>:
			func = new PartOfSpeechTag("p");
			parms.set(0, "My Bonnie <p>lies <b>over</b></p> the <b>ocean</b>.");
			result = func.exec(parms);
			assertTrue(matchOutput(result, new ArrayList<GroundTruth>() {
				{
					add(new GroundTruth("lies", "NNS"));
					add(new GroundTruth("over","IN"));
				};
			}));
			
			// HTML tag <body> wanted, but only <p> present:
			func = new PartOfSpeechTag("body");
			parms.set(0, "My Bonnie <p>lies over</p> the ocean.");
			result = func.exec(parms);
			assertNull(result);
			
			// HTML tag <body> wanted. A bunch of tags present inside. Should all be stripped:
			func = new PartOfSpeechTag("body");
			parms.set(0, "<body>My Bonnie <p>lies over</p> the <i>ocean</i>.</body>");
			result = func.exec(parms);

			assertTrue(matchOutput(result, new ArrayList<GroundTruth>() {
				{
					add(new GroundTruth("My", "PRP$"));
					add(new GroundTruth("Bonnie", "NNP"));
					add(new GroundTruth("lies", "VBZ"));
					add(new GroundTruth("over","IN"));
					add(new GroundTruth("ocean","NN"));					
				};
			}));
			
			// HTML tag <p>, and only output standard tags (as per
			// standardPartsOfSpeechToOutput in PartOrSpeechTag).
			// 
			func = new PartOfSpeechTag("p", true);
			parms.set(0, "My Bonnie <p>lies over</p> the ocean.");
			result = func.exec(parms);
			assertTrue(matchOutput(result, new ArrayList<GroundTruth>() {
				{
					add(new GroundTruth("lies", "NN"));
				};
			}));
			
			// No HTML tag consideration, and only output standard tags (as per
			// standardPartsOfSpeechToOutput in PartOrSpeechTag).
			func = new PartOfSpeechTag(true);
			parms.set(0, "My Bonnie <p>lies over</p> the ocean.");
			result = func.exec(parms);
			assertTrue(matchOutput(result, new ArrayList<GroundTruth>() {
				{
					add(new GroundTruth("Bonnie", "NNP"));
				    add(new GroundTruth("<p>", "NN"));
					add(new GroundTruth("lies", "VB"));
					add(new GroundTruth("</p>", "NN"));
					add(new GroundTruth("ocean", "NN"));
				};
			}));
			
			// No HTML tag consideration, and only output explicitly 
			// provided parts of speech tags:
			func = new PartOfSpeechTag(null, "NN");
			parms.set(0, "My Bonnie <p>lies over</p> the ocean.");
			result = func.exec(parms);
			assertTrue(matchOutput(result, new ArrayList<GroundTruth>() {
				{
				    add(new GroundTruth("<p>", "NN"));
					add(new GroundTruth("</p>", "NN"));
					add(new GroundTruth("ocean", "NN"));
				};
			}));
			
			// No HTML tag consideration, and only output explicitly 
			// provided parts of speech tags; more than one of those:
			func = new PartOfSpeechTag(null, "NN NNP");
			parms.set(0, "My Bonnie <p>lies over</p> the ocean.");
			result = func.exec(parms);
			assertTrue(matchOutput(result, new ArrayList<GroundTruth>() {
				{
					add(new GroundTruth("Bonnie", "NNP"));
				    add(new GroundTruth("<p>", "NN"));
					add(new GroundTruth("</p>", "NN"));
					add(new GroundTruth("ocean", "NN"));
				};
			}));
			
			// Only content of HTML <p> tag, and only output explicitly 
			// provided parts of speech tags; more than one of those:
			func = new PartOfSpeechTag("p", "NN NNP");
			parms.set(0, "My Bonnie <p>lies over the ocean.</p>");
			result = func.exec(parms);
			assertTrue(matchOutput(result, new ArrayList<GroundTruth>() {
				{
					add(new GroundTruth("ocean", "NN"));
				};
			}));
			
		} catch (IOException e) {
			System.out.println("Failed with IOException: " + e.getMessage());
			System.exit(-1);
		}
		
		System.out.println("All tests passed.");
	}
}
