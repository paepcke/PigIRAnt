package pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;

public class TestAnchorText {
	
	Properties props = new Properties();
	PigServer pserver = null;

	public TestAnchorText() {
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

			pserver.registerQuery(
					"docs = LOAD 'gov-03-2008:10' " +
					"USING pigir.webbase.WebBaseLoader() " +
					"AS (url:chararray, " +
					"	 date:chararray, " +
					"	 pageSize:int, " +
					"	 position:int, " +
					"	 docidInCrawl:int, " +
					"	 httpHeader:chararray, " +
					"	 content:chararray);"
			);
			
			pserver.registerQuery(
					"anchors = FOREACH docs GENERATE pigir.pigudf.AnchorText(content);");

			Common.print(pserver, "anchors");
					
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new TestAnchorText().doTest0();
	}
}
