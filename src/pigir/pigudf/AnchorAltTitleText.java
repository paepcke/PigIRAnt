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
 * Which of anchor text, alt text, and title text is included in
 * the ouput is determined by the constructor. Passing the string
 * "true" for the getAnchor, getAlt, and/or getTitle parameters
 * will causes the respective text fragments to be included. 
 * Similarly, passing the string "false" will suppress the respective
 * type of text fragment. 
 * 
 * @author paepcke
 *
 */
public class AnchorAltTitleText extends EvalFunc<Tuple> {
	
	public static boolean GET_ANCHOR_TEXT = true;
	public static boolean NO_ANCHOR_TEXT  = false;
	public static boolean GET_ALT_TEXT    = true;
	public static boolean NO_ALT_TEXT     = false;
	public static boolean GET_TITLE_TEXT  = true;
	public static boolean NO_TITLE_TEXT   = false;

	private final int GROUP_ANCHOR_TEXT = 2;
	private final int GROUP_ALT_TEXT    = 3;
	private final int GROUP_TITLE_TEXT  = 2;
	
	private boolean getAnchors = false;
	private boolean getAlts    = false;
	private boolean getTitles  = false;
	
	int MIN_LINK_LENGTH = "<a href=\"\"></a>".length();
    TupleFactory mTupleFactory = TupleFactory.getInstance();
	public final Logger logger = Logger.getLogger(getClass().getName());
	
	/**
	 * Constructor used to determine which types of text fragments are
	 * extracted from HTML. Note that at least one of the parameters
	 * must be the string "true".
	 * Note that these parameters are *strings*, not booleans. This is
	 *      because I don't see how to pass booleans from a Pig script.
	 *      
	 * @param getAnchor String "true" or "false" to control whether anchor text is included
	 * @param getAlt String "true" or "false" to control whether ALT tag text is included
	 * @param getTitle String "true" or "false" to control whether TITLE tag text is included
	 * @throws IOException 
	 */
	public AnchorAltTitleText(String getAnchor, String getAlt, String getTitle) throws IOException {
		if (getAnchor.equals("true"))
			getAnchors = true;
		if (getAlt.equals("true"))
			getAlts = true;
		if (getTitle.equals("true"))
			getTitles = true;
		if (!getAnchors && !getAlts && !getTitles)
			throw new IOException("At least one of getAnchor, getAlt, and getTitle must be the string 'true'.");
	}
	
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
    	
    	//if (getAnchorText) {
    	if (getAnchors) {
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
		
		//if (getAltText) {
    	if (getAlts) {
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
		
		//if (getTitleAttributeText) {
    	if (getTitles) {
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
