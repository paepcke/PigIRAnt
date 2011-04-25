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
 * Similarly:
 *    
 * Input: ({(Girl,d2,0),(child,d2,1),(lives,d2,2),(close,d2,3)})
 * would generate:
 *  (girl,child,1,d2
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
 * All of the following assumed parameters are set by 
 * the script makeWordPairDistances (in collaboration with
 * the companion script pigrun):
  
 *   $INDEX_FILE is set to the absolute HDFS path of the
 *                index file from which the distances are
 *                to be computed.
 *   $WORD_DISTS_DEST is the full path for the destination
 *                file.
 *   $PIG_HOME points to root of Pig installation
 *   $USER_CONTRIB points to location of PigIR.jar
 *   $USER_CONTRIB points to location of jsoup-1.5.2.jar
* 
 * @author "Andreas Paepcke"
 *
*/

 -- STORE command for the word distances:
%declare WORD_DISTANCES_STORE_COMMAND "STORE flatCohablist INTO '$WORD_DISTS_DEST' USING PigStorage(',');";

REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;
REGISTER $USER_CONTRIB/PigIR.jar;
REGISTER $USER_CONTRIB/jsoup-1.5.2.jar

/*
 * Read the index.
 * Structures will be tuples like: (word, docID, wordPosition)
 */
		docs = LOAD '$INDEX_FILE'
		USING PigStorage(',')
		AS (word:chararray,
			 docID:chararray,
			 wordPos:int);
					
/*
 Get a structure like this:
    ((d1,my),{(my,d1,3)})
    ((d1,man),{(man,d1,0)})
    ((d1,sun),{(sun,d1,2)})
    ((d1,lives),{(lives,d1,1),(lives,d1,5)})
    ((d2,me),{(me,d2,6)})
    ((d2,Girl),{(Girl,d2,0)})
*/
docsGroupedDocIDWord = GROUP docs BY (docID,word);
			
/*
Get a structure like this:
	(d1,{(man,d1,0),(lives,d1,1),(sun,d1,2)})
	(d2,{(Girl,d2,0),(child,d2,1),(lives,d2,2),(close,d2,3)})
Schema:
  docsGroupedDocID: {group: chararray,docs: {word: chararray,docID: chararray,wordPos: int}}
*/

docsGroupedDocID = GROUP docs BY (docID);
			
/* Get the first half of the co-occurrence matrix
* by passing the bag part in each of the docsCgroupedDocID tuples
* to the MakeWordPairDistances() UDF. We well get back:
*    ((word1, word2, distance, docID), (word1, word2, distance, docID), ...)
* The schema return by MakeWordPairDistance() is:
*    cohablist: {distances::word1: chararray,distances::word2: chararray,distances::distance: int,distances::docID: chararray}
*/

cohablist = FOREACH docsGroupedDocID GENERATE flatten(pigir.pigudf.MakeWordPairDistances(docs));
flatCohablist = FOREACH cohablist GENERATE FLATTEN(org.apache.pig.piggybank.evaluation.util.ToBag(*));

--sorted    = ORDER flatCohablist BY $0;

--DUMP flatCohablist;
$WORD_DISTANCES_STORE_COMMAND;
