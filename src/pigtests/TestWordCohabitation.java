package pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;

/**
 * Starting with an index built by, for example, buildWebBaseIndex,
 * generate a word distance structure. For example, starting with
 * an index for these two documents:
 * 
 * Doc1:
 *    This man lives in the sun.
 *    My wife lives near the train station.
 *
 * Doc2:
 *    Girl child lives close.
 *    Wife near me lives station of train.
 *
 * @author "Andreas Paepcke"
 *
 */
public class TestWordCohabitation {
	
	Properties props = new Properties();
	PigServer pserver = null;

	public TestWordCohabitation() {
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			pserver = new PigServer(ExecType.MAPREDUCE, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
	}
		
	public void doTest0() {
		
		try {

			Map<String, String> env = System.getenv();
			URI piggybankPath = new File(env.get("PIG_HOME"),
					"contrib/piggybank/java/piggybank.jar").toURI();
			pserver.registerJar(piggybankPath.toString());
			pserver.registerJar("contrib/PigIR.jar");

			/*
			 * Read the index.
			 * Structures will be tuples like: (word, docID, wordPosition)
			 */
			pserver.registerQuery(
					"docs = LOAD 'Datasets/tinyIndex.idx' " +
					"USING PigStorage(',') " +
					"AS (word:chararray, " +
					"	 docID:chararray, " +
					"	 wordPos:int);"
			);
					
			/*
			  Get a structure like this:
			     ((d1,my),{(my,d1,3)})
                 ((d1,man),{(man,d1,0)})
			     ((d1,sun),{(sun,d1,2)})
			     ((d1,lives),{(lives,d1,1),(lives,d1,5)})
			     ((d2,me),{(me,d2,6)})
			     ((d2,Girl),{(Girl,d2,0)})
			 */
			pserver.registerQuery(
					"docsGroupedDocIDWord = GROUP docs BY (docID,word);"
			);
			
			/*
			Get a structure like this:
				(d1,{(man,d1,0),(lives,d1,1),(sun,d1,2)})
				(d2,{(Girl,d2,0),(child,d2,1),(lives,d2,2),(close,d2,3)})
			Schema:
			  docsGroupedDocID: {group: chararray,docs: {word: chararray,docID: chararray,wordPos: int}}
			*/

			pserver.registerQuery(
					"docsGroupedDocID = GROUP docs BY (docID);"
			);
//---> Call zipper here: just pass in the bag ($1)	
			/*
			 * Get this structure (notice that all is lower case now): 
			     (d1,{(man),(lives),(sun)},{(0),(1),(2)})
			     (d2,{(girl),(child),(lives),(close),(wife)},{(0),(1),(2),(3),(4)})
				wordGroups: {group: chararray,word: {word: chararray},wordPos: {wordPos: int}}
 
			 */
			
			pserver.registerQuery(
					"wordGroups = FOREACH docsGroupedDocID GENERATE group, docs.word, docs.wordPos;"
			);
			
			//Common.print(pserver, "docsGroupedDocIDWord");
			//Common.print(pserver, "docsGroupedDocID");
			//pserver.dumpSchema("docsGroupedDocID");
			Common.print(pserver, "wordGroups");
			pserver.dumpSchema("wordGroups");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new TestWordCohabitation().doTest0();
		//new TestIndexOneDocument().doTests2();
	}
}
