/*
Given the contact information to a crawl, produce
a CSV output in which each item is one of:

   o A Web page's anchor texts
   o A Web page's ALT tag texts
   o A Web page's TITLE tag texts
   
Note that the TITLE tag is not the HTML page's title.
It's the TITLE tag that may be added to HTML elements
as an attribute to provide roll-over explanations.

   Start this script via webBaseAnchorAltTitle, and see
   usage help for that script for details.
    
    Environment expectations:
      * $PIG_HOME points to root of Pig installation
      * $USER_CONTRIB points to location of PigIR.jar
      * $USER_CONTRIB points to location of jsoup-1.5.2.jar

   $PIG_HOME and $USER_CONTRIB are assumed to be passed in
   via -param command line parameters. The pigrun script that
   is used by webBaseAnchorAltTitle takes care of this. Additionally,
   the following env vars must be passed in via -param:
   
       $CRAWL_SOURCE=crawl source, page numbers, start/stop sites as per examples above
       $TEXT_FRAGMENT_DEST path to where results are to go
       $EXTRACT_CALL: the call to the AnchorAltTitle.java UDF.
            ex: pigir.pigudf.AnchorAltTitle(content,true,false,false)
       
   The webBaseAnchorAltTitle script constructs all these parameters from its
   command line parameters.
    
*/

-- STORE command for the extracted fragments:
%declare TEXT_FRAGMENTS_STORE_COMMAND "STORE fragments INTO '$TEXT_FRAGMENT_DEST' USING org.apache.pig.piggybank.storage.CSVExcelStorage();";
REGISTER $PIG_HOME/contrib/piggybank/java/piggybank.jar;
REGISTER $USER_CONTRIB/PigIR.jar;
REGISTER $USER_CONTRIB/jsoup-1.5.2.jar

define anchorAltTitleText pigir.pigudf.AnchorAltTitleText('$GET_ANCHORS','$GET_ALTS','$GET_TITLES'); 

--docs = LOAD 'gov-03-2007:2'
docs = LOAD '$CRAWL_SOURCE'
	USING pigir.webbase.WebBaseLoader()
	AS (url:chararray,
	    date:chararray,
	 	pageSize:int,
	 	position:int,
	 	docIDInCrawl:int,
	 	httpHeader,
	 	content:chararray);

fragments = FOREACH docs GENERATE anchorAltTitleText(content);	

DUMP fragments;
--$TEXT_FRAGMENT_STORE_COMMAND;

 	