-- STORE command for the word count:
%declare WORD_COUNT_STORE_COMMAND "STORE sorted INTO '$ANCHOR_TEXT_DEST' USING PigStorage(',');";

REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;
REGISTER $USER_CONTRIB/PigIR.jar;
REGISTER $USER_CONTRIB/jsoup-1.5.2.jar

docs = LOAD '$CRAWL_SOURCE'
	USING pigir.webbase.WebBaseLoader()
	AS (url:chararray,
	    date:chararray,
	 	pageSize:int,
	 	position:int,
	 	docIDInCrawl:int,
	 	httpHeader,
	 	content:chararray);

anchors = FOREACH docs GENERATE pigir.pigudf.AnchorText(content);	

DUMP anchors;
 	