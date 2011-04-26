package pigir.pigudf.unittests;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import pigir.pigudf.StripHTML;


public class TestStripHTML {

	public static void main(String[] args) throws ExecException {
		
		StripHTML func = new StripHTML();
		TupleFactory tupleFac = TupleFactory.getInstance();
		Tuple parms = tupleFac.newTuple(1);
		String htmlStr = "<head><html>This is <b>bold</b> and a <a href='http://test.com'>link anchor</a></html></head>";
		String groundTruth = "This is bold and a link anchor";
		
		try {
			// Something normal:
			parms.set(0, htmlStr);
			assertEquals(groundTruth, func.exec(parms));
			
			// Upper case HREF:
			htmlStr = "<head><html>This is <b>bold</b> and a <a HREF='http://test.com'>link anchor</a></html></head>";
			assertEquals(groundTruth, func.exec(parms));
			
			// Empty string:
			parms.set(0,"");
			assertEquals("",func.exec(parms));

			// Access to html stripping via the StripHTML class's static
			// extractText() method:
			assertEquals(groundTruth, StripHTML.extractText(htmlStr));
			
			//????
			htmlStr = "On a <a href=\"http://foo/bar.html\">sunny</a> day in <a href=\"https:8090//blue/bar?color=green\">in March   </a> we ran.";
			System.out.println(StripHTML.extractText(htmlStr));
			
		} catch (IOException e) {
			System.out.println("Failed with IOException: " + e.getMessage());
			System.exit(-1);
		}
		
		System.out.println("All tests passed.");
	}
}