package pigtests;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;

import pigir.Common;

class QuickTests {
	PigServer pserver;
	Properties props = new Properties();

	public QuickTests() {
/*
		try {
			props.setProperty("pig.usenewlogicalplan", "false");
			//pserver = new PigServer(ExecType.MAPREDUCE, props);
			pserver = new PigServer(ExecType.LOCAL, props);
		} catch (ExecException e) {
			e.printStackTrace();
		}
*/		
	}
	
	public void doTests() {
		//String html="Foo<a href=\"http://access.usgs.gov/\"><img src=\"images/headers/pes-text-title-green.gif\" alt=\"Priority Ecosystems Science Initiative\" height=\"30\" width=\"324\" border=\"0\"></a>bar";
		String html="Foo<a HREF=\"http://access.usgs.gov/\"><img src=\"images/headers/pes-text-title-green.gif\" alt=\"Priority Ecosystems Science Initiative\" height=\"30\" width=\"324\" border=\"0\"></a>bar";
		
		//Pattern pattern = Pattern.compile("<a[\\s]+href[\\s]*=[\\s]*\"[^>]*>(.*?)</a>");
		Pattern pattern = Pattern.compile("<a[\\s]+(href|HREF)[\\s]*=[\\s]*\"[^>]*>(.*?)</a>");
		Matcher matcher = pattern.matcher(html);
		while(matcher.find()){
			System.out.println(matcher.group(2));
		}
		
	}
	public static void main(String[] args) {
		new QuickTests().doTests();
	}
 }


