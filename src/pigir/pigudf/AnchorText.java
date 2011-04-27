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

/**
 * Given an HTML string, return a tuple of strings that comprise
 * the anchor texts in the HTML. All HTML tags are stripped from
 * the anchor texts. Optionally two more text fragments can 
 * be extracted: the body of the ALT attribute to the image
 * element, and the body of the TITLE attribute to other
 * HTML elements. The latter is not to be confused with the
 * HTML page title. {@link http://www.w3schools.com/tags/att_standard_title.asp}.
 * 
 * NOTE: does not properly handle embedded escaped double quotes.
 *         <... ALT="foo \"bar\" baz"...>
 *       will return:
 *         foo \
 * 
 * @author paepcke
 *
 */
public class AnchorText extends EvalFunc<Tuple> {
	
	public static boolean GET_ANCHOR_TEXT = true;
	public static boolean NO_ANCHOR_TEXT  = false;
	public static boolean GET_ALT_TEXT    = true;
	public static boolean NO_ALT_TEXT     = false;
	public static boolean GET_TITLE_TEXT  = true;
	public static boolean NO_TITLE_TEXT   = false;

	
	private final int GROUP_ANCHOR_TEXT = 2;
	private final int GROUP_ALT_TEXT    = 3;
	private final int GROUP_TITLE_TEXT  = 2;
	
	int MIN_LINK_LENGTH = "<a href=\"\"></a>".length();
    TupleFactory mTupleFactory = TupleFactory.getInstance();
	public final Logger logger = Logger.getLogger(getClass().getName());
	
    /**
     * Main method for extracting anchor text, alt text, and title 
     * attribute text.
     * @param input Single-field tuple with an HTML string
     * @param getAnchorText Boolean with value AnchorText.GET_ANCHOR_TEXT, or AnchorText.NO_ANCHOR_TEXT.
     * @param getAltText Boolean with value AnchorText.GET_ALT_TEXT, or AnchorText.NO_ALT_TEXT.
     * @param getTitleAttributeText getAltText Boolean with value AnchorText.GET_TITLE_TEXT, or AnchorText.NO_TITLE_TEXT.
     * @return Tuple of strings with all extracted text fragments. 
     * @throws IOException
     */
    public Tuple exec(Tuple input, boolean getAnchorText, boolean getAltText, boolean getTitleAttributeText) throws IOException {
    	
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
    	
    	if (getAnchorText) {
    		// Matcher to extract anchor text. The '?' after the .* 
    		// before the </a> turns this  match non-greedy. Without 
    		// the question mark, the .* would eat all the html to 
    		// the last </a>. We look for a not-escaped opening angle
    		// bracket, followed by the HREF tag name, etc.:
    		Pattern anchorTextPattern = Pattern.compile("[^\\\\]<a[\\s]+(href|HREF|Href)[\\s]*=[\\s]*\"[^>]*>(.*?)</a>");
    		Matcher anchorTextMatcher = anchorTextPattern.matcher(html);		
    		while(anchorTextMatcher.find()){
    			output.append(StripHTML.extractText(anchorTextMatcher.group(GROUP_ANCHOR_TEXT)));
    		}
    	}
		
		if (getAltText) {
			// Matcher to extract text of the alt attribute. None-escaped left angle
			// plus some form of the tags IMG, AREA, or INPUT.
			// The question mark after the * near the end
			// of the following pattern prevents the matcher
			// from gobbling up everything up to the last double
			// quote. We only want to go as far as the double quote
			// that ends the ALT text (i.e. the question mark turns
			// off greedy matching):

			// NOTE: this pattern does not properly handle 
			//       when ALT text contains an escaped double quote.
			//            <img ... ALT="Foo\"bar\"> will 
			//       correctly return 
			//            Foo \

			Pattern altTextPattern = Pattern.compile("[^\\\\]<*(img|IMG|Img|area|AREA|Area|input|INPUT|Input).*(alt|ALT|Alt)[\\s]*=[\\s]*\"(([^\">]|[\\\"])*?)\"");
			Matcher altTextMatcher = altTextPattern.matcher(html);
			while(altTextMatcher.find()){
				output.append(StripHTML.extractText(altTextMatcher.group(GROUP_ALT_TEXT)));
			}
		}
			//Foo <img alt="Fun image." src="http://foo/bar"> <b TITLE="Bold for emphasis."> <a href="http://www.blue/red" TITLE="This is a title text.">body</a>
		
		if (getTitleAttributeText) {
			// Matcher to extract the 'title' attribute: <element title="Tooltip or similar text">. 
			// That is, not the title element of the entire HTML page:
			// Start looking for a not-escaped opening tag:
			Pattern titleTextPattern = Pattern.compile("[^\\\\]<*(title|TITLE|Title)[\\s]*=[\\s]*\"(([^\"]|[\\\"])*?)\"");
			Matcher titleTextMatcher  = titleTextPattern.matcher(html);
			while(titleTextMatcher.find()){
				output.append(StripHTML.extractText(titleTextMatcher.group(GROUP_TITLE_TEXT)));
			}
		}
		
		return output;
    }
    
    /* (non-Javadoc)
     * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
     * Exctract anchor text from HTML string.
     * @param input Single-field tuple with an HTML string
     * @return Tuple of anchor text strings.
     */
    public Tuple exec(Tuple input) throws IOException {
    	return exec(input, GET_ANCHOR_TEXT, NO_ALT_TEXT, NO_TITLE_TEXT);
    }
    
    /**
     * Extract anchor text and/or ALT attribute body text from HTML string.
     * @param input One-tuple with HTML string
     * @param getAnchorText Boolean to control whether Anchor attribute body text is to be extracted. Values: 
     * 						AnchorText.GET_ANCHOR_TEXT, or AnchorText.NO_ANCHOR_TEXT.
     * @param getAltText Boolean to control whether ALT attribute body text is to be extracted. Values:
     * 						AnchorText.GET_ALT_TEXT, or AnchorText.NO_ALT_TEXT.
     * @return Tuple of strings with the requested text fragments.
     * @throws IOException
     */
    public Tuple exec(Tuple input, boolean getAnchorText, boolean getAltText) throws IOException {
    	return exec(input, getAnchorText, getAltText, NO_TITLE_TEXT);
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
        
        // Call with two parameters: an HTML string, and a boolean to 
        // control whether ALT text should also be extracted (default is false):
        List<FieldSchema> twoParmsFieldSchema = new ArrayList<FieldSchema>(2);
        twoParmsFieldSchema.add(new FieldSchema(null, DataType.CHARARRAY));  // the HTML document
        twoParmsFieldSchema.add(new FieldSchema(null, DataType.BOOLEAN));    // the ALT text yes/no
        FuncSpec twoParms = new FuncSpec(this.getClass().getName(), new Schema(twoParmsFieldSchema));
        funcSpecs.add(twoParms);
        
        // Call with three parameters: an HTML string, a boolean to 
        // control whether ALT text should also be extracted (default is false),
        // and a second boolean to control whether TITLE attribute text
        // should also be extracted (default is false):
        List<FieldSchema> threeParmsFieldSchema = new ArrayList<FieldSchema>(3);
        threeParmsFieldSchema.add(new FieldSchema(null, DataType.CHARARRAY));  // the HTML document
        threeParmsFieldSchema.add(new FieldSchema(null, DataType.BOOLEAN));    // the ALT text yes/no
        threeParmsFieldSchema.add(new FieldSchema(null, DataType.BOOLEAN));    // the TITLE text yes/no
        FuncSpec threeParms = new FuncSpec(this.getClass().getName(), new Schema(threeParmsFieldSchema));
        funcSpecs.add(threeParms);
        
        return funcSpecs;
    }
}
