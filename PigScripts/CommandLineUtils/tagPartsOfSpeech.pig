
-- STORE command for the parts of speech file:
%declare POS_STORE_COMMAND "STORE partsOfSpeech INTO '$DEST_FILE' USING PIGStorage(',');";

REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;
REGISTER $USER_CONTRIB/PigIR.jar;
REGISTER $USER_CONTRIB/jsoup-1.5.2.jar;
REGISTER $USER_CONTRIB/stanford-postagger-2011-04-20_with_model.jar;


define partOfSpeechTag pigir.pigudf.PartOfSpeechTag('$TAG_FILTER','$MAIN_POS','$POS_TAGS'); 

docs = LOAD '$CRAWL_SOURCE'
	USING pigir.webbase.WebBaseLoader()
	AS (url:chararray,
	    date:chararray,
	 	pageSize:int,
	 	position:int,
	 	docIDInCrawl:int,
	 	httpHeader,
	 	content:chararray);

partsOfSpeech = FOREACH docs GENERATE FLATTEN(partOfSpeechTag(content));

DUMP partsOfSpeech;
--$POS_STORE_COMMAND;

