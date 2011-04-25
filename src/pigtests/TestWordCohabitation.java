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
 * generate a word distance file. For example, starting with
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
 * the partial index would look like this:
 * 
* Input : ({(man,d1,0),(lives,d1,1),(sun,d1,2)})
* 
* and this word distances script would output:
*    (man,lives,1,d1)
*    (man,sun,2,d1)
*    (lives,sun,1,d1)
*    
* Input: ({(Girl,d2,0),(child,d2,1),(lives,d2,2),(close,d2,3)})
* would generate:
*   (girl,child,1,d2
*	(child,lives,1,d2
*	(girl,lives,2,d2
*	(lives,close,1,d2
*	(child,close,2,d2
*	(girl,close,3,d2)
*
* Notice that 'Girl' is lower-case in the output. This
*        is true for all entries.
*        
* The maximum distance between words that will still output
* these distance tuples is set in the UDF MakeWordPairDistances.java.
* Default is a distance of 5 (inclusive).
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
			
			/* Get the first half of the co-occurrence matrix
			 * by passing the bag part in each of the docsCgroupedDocID tuples
			 * to the MakeWordPairDistances() UDF. We well get back:
			 *    ((word1, word2, distance, docID), (word1, word2, distance, docID), ...)
			 * The schema return by MakeWordPairDistance() is:
			 *    cohablist: {distances::word1: chararray,distances::word2: chararray,distances::distance: int,distances::docID: chararray}
			 */
			pserver.registerQuery(
					"cohablist = FOREACH docsGroupedDocID GENERATE flatten(pigir.pigudf.MakeWordPairDistances(docs));"
			);
			
			/* We now need the reflection of this half-matrix, so that for each
			 * word1/word2 pair, we can also find word2/word1:
			 */
			
			//pserver.registerQuery(
			//		"reflectionCohabList = FOREACH cohablist GENERATE word2, word1,distance,docID;"
			//);
			
			pserver.registerQuery(
					"cohablistSorted = ORDER cohablist BY word1, word2;"
			);
			
			
			//Common.print(pserver, "docsGroupedDocIDWord");
			//Common.print(pserver, "docsGroupedDocID");
			//pserver.dumpSchema("docsGroupedDocID");
			//Common.print(pserver, "wordGroups");
			//Common.print(pserver, "cohablist");
			//Common.print(pserver, "reflectionCohabList");
			Common.print(pserver, "cohablistSorted");
			pserver.dumpSchema("cohablistSorted");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new TestWordCohabitation().doTest0();
		//new TestIndexOneDocument().doTests2();
	}
}
