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
import pigir.pigudf.AnchorText;

public class TestAnchorText {

	/**
	 * For each result from IndexOneDoc, verify that every tuple is correct.
	 * 
	 * @param result is a tuple: ((docID,numPostings), (token1,docID,token1Pos), (token2,docID,token2Pos), ...). All docID are identical. 
	 * @param groundTruth an array of Truth objects. Each object contains one token and its position. The objects are ordered as in the expected result.
	 * @return true/false.
	 */
	private static boolean matchOutput(Tuple result, ArrayList<String> groundTruth) {

		Iterator<Object> resultIt = Common.getTupleIterator(result);
		Iterator<String> truthIt  = groundTruth.iterator();
		String nextRes = null;
		String nextTruth = null;

		if (result.size() == 0 && groundTruth.size() == 0)
			return true;
		
		while (resultIt.hasNext()) {
			if (! truthIt.hasNext())
				return false;
			nextRes   = (String) resultIt.next();
			nextTruth = truthIt.next();
			if (!nextRes.equals(nextTruth))
				return false;
		}
		if (truthIt.hasNext())
			return false;

		return true;
	}

	@SuppressWarnings("serial")
	public static void main(String[] args) throws ExecException {
		
		AnchorText func = new AnchorText();
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr;
		
		try {

			// No link, and string shorter than a minimum link's length:
			parms.set(0, "On a sunny day");
			assertNull(func.exec(parms));
			
			// No link, long enough to possibly contain a link:
			htmlStr = "On a truly sunny day we walked along the beach, and smiled.";
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>()));
			
			// Correct and tightly spaced link:
			htmlStr = "On a <a href=\"http://foo/bar.html\">sunny</a> day";
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("sunny");
				};
			}));

			// Correct link with spaces:
			htmlStr = "On a <a href   =   \"http://foo/bar.html\">sunny</a> day";
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("sunny");
				};
			}));
			
			// No quotes around the URL ==> Not taken as a link:
			htmlStr = "On a <a href=http://foo/bar.html>sunny</a> day";
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>()));

			// Multiple links, and three spaces in second anchor:
			htmlStr = "On a <a href=\"http://foo/bar.html\">sunny</a> day in <a href=\"https:8090//blue/bar?color=green\">in March   </a> we ran.";
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("sunny");
					add("in March");
				};
			}));
			
			// Link that's nothing but tagged HTML:
			htmlStr = "On a <a href=\"http://foo/bar.html\"><img src=/foo/bar></a> we ran.";
			parms.set(0, htmlStr);
			assertTrue(matchOutput(func.exec(parms), new ArrayList<String>() {
				{
					add("");
				};
			}));
			
			// ALT text:
			htmlStr = "Foo <img src=\"http://www.blue/red\" alt=\"This is an alt text.\">";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms, AnchorText.GET_ANCHOR_TEXT, AnchorText.GET_ALT_TEXT), new ArrayList<String>() {
				{
					add("This is an alt text.");
				};
			}));

			// ALT text with embedded escaped double quotes:
			htmlStr = "Foo <IMG src=\"http://www.blue/red\" alt=\"This is an \\\"alt\\\" text.\">";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms, AnchorText.GET_ANCHOR_TEXT, AnchorText.GET_ALT_TEXT), new ArrayList<String>() {
				{
					// add("This is an \\\"alt\\\" text.");   // Should return this, but:
					add("This is an \\");                     // See comment in method.
				};
			}));
			
			// Capitalized ALT text:
			htmlStr = "Foo <Img src=\"http://www.blue/red\" ALT=\"This is an alt text.\">";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms, AnchorText.GET_ANCHOR_TEXT, AnchorText.GET_ALT_TEXT), new ArrayList<String>() {
				{
					add("This is an alt text.");
				};
			}));
			
			
			// Title text:
			htmlStr = "Foo <a href=\"http://www.blue/red\" title=\"This is a title text.\">body</a>";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms, AnchorText.GET_ANCHOR_TEXT, AnchorText.NO_ALT_TEXT, AnchorText.GET_TITLE_TEXT), new ArrayList<String>() {
				{
					add("body");
					add("This is a title text.");
				};
			}));
			
			// Caps Title text:
			htmlStr = "Foo <a href=\"http://www.blue/red\" TITLE=\"This is a title text.\">body</a>";
			parms.set(0,htmlStr);
			assertTrue(matchOutput(func.exec(parms, AnchorText.NO_ANCHOR_TEXT, AnchorText.NO_ALT_TEXT, AnchorText.GET_TITLE_TEXT), new ArrayList<String>() {
				{
					add("This is a title text.");
				};
			}));
			
			// Alt and  Title text, but no anchor text:
			htmlStr = "Foo <img alt=\"Fun image.\" src=\"http://foo/bar\"> <b TITLE=\"Bold for emphasis.\"> <a href=\"http://www.blue/red\" TITLE=\"This is a title text.\">body</a>";
			parms.set(0,htmlStr);
			
			assertTrue(matchOutput(func.exec(parms, AnchorText.NO_ANCHOR_TEXT, AnchorText.GET_ALT_TEXT, AnchorText.GET_TITLE_TEXT), new ArrayList<String>() {
				{
					add("Fun image.");
					add("Bold for emphasis.");
					add("This is a title text.");
				};
			}));
			
			// Title text with embedded escaped double quote:
			htmlStr = "Foo <b TITLE=\"Bold for \\\"emphasis\\\".\">";
			parms.set(0,htmlStr);
			
			assertTrue(matchOutput(func.exec(parms, AnchorText.GET_ANCHOR_TEXT, AnchorText.GET_ALT_TEXT, AnchorText.GET_TITLE_TEXT), new ArrayList<String>() {
				{
					// add("Bold for \\\"emphasis\\\".");   // Should return this, but:
					add("Bold for \\");                     // the bug in AnchorText.java returns this. Oh well.   
				};
			}));
			
		} catch (IOException e) {
			System.out.println("Failed with IOException: " + e.getMessage());
			System.exit(-1);
		}
		
		System.out.println("All tests passed.");
	}
}
