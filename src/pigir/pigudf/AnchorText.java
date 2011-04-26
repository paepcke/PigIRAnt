package pigir.pigudf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

// TODO: HREF is missed
//       de-tag anchor text; throw out images
//       grap alt text


/**
 * Given an HTML string, return a tuple of strings that comprise
 * the anchor texts in the HTML. All HTML tags are stripped from
 * the anchor texts.
 * 
 * @author paepcke
 *
 */
public class AnchorText extends EvalFunc<Tuple> {
	
	private final int GROUP_ANCHOR_TEXT = 1;
	private final int GROUP_ALT_TEXT    = 2;
	private final int GROUP_TITLE_TEXT  = 2;
	
	int MIN_LINK_LENGTH = "<a href=\"\"></a>".length();
    TupleFactory mTupleFactory = TupleFactory.getInstance();
	public final Logger logger = Logger.getLogger(getClass().getName());
	
	// The '?' after the .* before the </a> turns this 
	// match non-greedy. Without the question mark, the
	// .* would eat all the html to the last </a>:
	private Pattern anchorTextPattern = Pattern.compile("<a[\\s]+href[\\s]*=[\\s]*\"[^>]*>(.*?)</a>");
	
	// Matcher to extract text of the alt attribute:
	private Pattern altTextPattern = Pattern.compile("<a[\\s]+href.*(alt|ALT|Alt)[\\s]*=[^\\s]*\"(([^\"]|[\\\"])*)\"");
	
	// Matcher to extract the 'title' attribute: <element title="Tooltip or similar text">. 
	// That is, not the title element of the entire HTML page:
	private Pattern titleTextPattern = Pattern.compile("[^\\\\]<a.*(title|TITLE|Title)[\\s]*=[^\\s]*\"(([^\"]|[\\\"])*)\"");
    
    public Tuple exec(Tuple input) throws IOException {
    	
    	String html = null;
    	Tuple output = mTupleFactory.newTuple();
		
    	try {
    		if ((input.size() == 0) || 
    				((html = (String) input.get(0)) == null) ||
    				(html.length() < MIN_LINK_LENGTH)) {
    			return null;
    		}
    	} catch (ClassCastException e) {
    		throw new IOException("AnchorText(): bad input: " + input);
    	}
		Matcher anchorTextMatcher = anchorTextPattern.matcher(html);
		Matcher altTextMatcher    = altTextPattern.matcher(html);
		Matcher titleTextMatcher  = titleTextPattern.matcher(html);
		
		while(anchorTextMatcher.find()){
			output.append(StripHTML.extractText(anchorTextMatcher.group(GROUP_ANCHOR_TEXT)));
		}
		while(altTextMatcher.find()){
			output.append(StripHTML.extractText(altTextMatcher.group(GROUP_ALT_TEXT)));
		}
		while(titleTextMatcher.find()){
			output.append(StripHTML.extractText(titleTextMatcher.group(GROUP_TITLE_TEXT)));
		}
		
		return output;
    }
    
	public Schema outputSchema(Schema input) {
        try{
            Schema anchorTextSchema = new Schema();
        	anchorTextSchema.add(new Schema.FieldSchema("text", DataType.CHARARRAY));
        	
        	// Schema of all anchor text strings:
        	Schema outSchema = new Schema(new Schema.FieldSchema("anchorTexts", anchorTextSchema, DataType.TUPLE));
            return outSchema;
        }catch (Exception e){
                return null;
        }
    }
	
    @Override
    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
    	
        List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>();
        
        // Call with one parameter: an HTML string:
        List<FieldSchema> htmlFieldSchema = new ArrayList<FieldSchema>(1);
        htmlFieldSchema.add(new FieldSchema(null, DataType.CHARARRAY));  // the HTML document
        FuncSpec htmlParameterOnly = new FuncSpec(this.getClass().getName(), new Schema(htmlFieldSchema));
        funcSpecs.add(htmlParameterOnly);
        
        return funcSpecs;
    }
}
