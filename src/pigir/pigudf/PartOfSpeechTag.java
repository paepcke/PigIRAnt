package pigir.pigudf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
	

/**
 * Pig UDF function to tag text with parts of speech markers.
 * The function takes either plain text, or HTML/XML. The output
 * is a tuple of 2-tuples. Each 2-tuple consists of one word from
 * the given text, and one part of speech tag (defined below).  
 * <p>
 * The following options are available in various combinations:
 * <ul>
 * <li>Only generate output for text within a given HTML/XML tag.
 * <li>Only generate output for a small number of POS tags, as
 * 	   defined in standardPartsOfSpeechToOutput below. The different
 * 	   variants of grammatical entities are mapped into simple forms.
 *     For example: Noun singular, and noun plural are both tagged
 *     with Noun singluar (NN) for this option. 
 * <li>Only generate output for an explicitly provided set of parts of speech tags.
 * </ul>
 * 
 * List of POS tags:
 * <ol>
 * <li>CC Coordinating conjunction
 * <li>CD Cardinal number
 * <li>DT Determiner
 * <li>EX Existential there
 * <li>FW Foreign word
 * <li>IN Preposition or subordinating conjunction
 * <li>JJ Adjective
 * <li>JJR Adjective, comparative
 * <li>JJS Adjective, superlative
 * <li>LS List item marker
 * <li>MD Modal
 * <li>NN Noun, singular or mass
 * <li>NNS Noun, plural
 * <li>NNP Proper noun, singular
 * <li>NNPS Proper noun, plural
 * <li>PDT Predeterminer
 * <li>POS Possessive ending
 * <li>PRP Personal pronoun
 * <li>PRP$ Possessive pronoun
 * <li>RB Adverb
 * <li>RBR Adverb, comparative
 * <li>RBS Adverb, superlative
 * <li>RP Particle
 * <li>SYM Symbol
 * <li>TO to
 * <li>UH Interjection
 * <li>VB Verb, base form
 * <li>VBD Verb, past tense
 * <li>VBG Verb, gerund or present participle
 * <li>VBN Verb, past participle
 * <li>VBP Verb, non­3rd person singular present
 * <li>VBZ Verb, 3rd person singular present
 * <li>WDT Wh­determiner
 * <li>WP Wh­pronoun
 * <li>WP$ Possessive wh­pronoun
 * <li>WRB Wh­adverb
 * </ol>
 * 
 * @author "Andreas Paepcke"
 *
 */
public class PartOfSpeechTag  extends EvalFunc<Tuple>  {
	
	public final Logger logger = Logger.getLogger(getClass().getName());
	private String modelPath = "jar:left3words-wsj-0-18.tagger";
	private String HTMLTagsToInclude  = "";
	private static MaxentTagger tagger = null;
	private HashMap<String,String> partsOfSpeechToOutput = null;
	@SuppressWarnings("serial")
	private HashMap<String,String> standardPartsOfSpeechToOutput = new HashMap<String,String>() {
		{
			put("NN", "NN");      // Noun singular
			put("NNS", "NN");     // Noun plural
			put("NNP", "NNP");    // Proper noun
			put("NNPS", "NNP");   // Proper noun plural
			put("RB", "RB");      // Adverb
			put("RBR", "RB");     // Adverb comparative
			put("RBS", "RB");     // Adverb superlative
			put("JJ", "JJ");      // Adjective
			put("JJR", "JJ");     // Adjective comparative
			put("JJS", "JJ");     // Adjective superlative
			put("VB", "VB");      // Verb base form
			put("VBD", "VB");     // Verb past tense
			put("VBG", "VB");     // Verb gerund or present participle
			put("VBN", "VB");     // Verb past participle
			put("VBP", "VB");     // Verb non-3rd person singular present
			put("VBZ", "VB");     // Verb 3rd person singular present
		}
	};
	private boolean filterPOSTags = false;

	/*-------------------------------------
	* Constructors
	*---------------*/
	
	/**
	 * No special treatment of HTML, output only 
	 * parts of speech defined in standardPartsOfSpeechToOutput:
	 */
	public PartOfSpeechTag() {
		this(null, "true", null);
	}
	
	/**
	 * Only tag content within the given HTML tags, 
	 * output only the parts of speech tags given
	 * in standardPartsOfSpeechToOutput:
	 */
	public PartOfSpeechTag(String theHTMLTags) {
		this(theHTMLTags, "true", null);
	}
	
	/**
	 * Only tag content within the given HTML tags, output the 
	 * parts of speech tags given in POSTasToOutput:
	 */
	public PartOfSpeechTag(String theHTMLTags, String POSTagsToOutput) {
		this(theHTMLTags, "false", POSTagsToOutput);
	}
	
	/**
	 * @param theHTMLTags. If theHTMLTags is a string of tags, those are passed into the
	 * 		call to runTagger() of the POS tagger. 
	 * @param outputStandardPOSTags. If outputStandardPOSTags is
	 * 		TRUE, then only words tagged with the parts of speech defined in
	 * 		the HashMap standardPartsOfSpeechToOutput will be included in the
	 * 		output.  
	 * @param POSTagsToOutput. If this is a non-empty string, and outputStandardPOSTags
	 * 		is false, then the string is expected to be a space separated list
	 * 		of parts of speech tags. Only words tagged with tags in this parameter will be
	 * 		included in the output.
	 */
	public PartOfSpeechTag(String theHTMLTags, String outputStandardPOSTags, String POSTagsToOutput) {

		// It's tricky to pass non-strings from a Pig script
		// into a UDF. Also, it's tricky to pass an empty string
		// from a bash script into a Pig script. So we allow the 
		// string 'null' to indicate an empty string: 
		if ((theHTMLTags != null) && 
			!theHTMLTags.trim().isEmpty() &&
			!theHTMLTags.equalsIgnoreCase("null"))
			HTMLTagsToInclude = theHTMLTags;
		
		if (outputStandardPOSTags.equals("true")) {
			partsOfSpeechToOutput = standardPartsOfSpeechToOutput;
			filterPOSTags = true;
		} else if ((POSTagsToOutput != null) && 
					!POSTagsToOutput.trim().isEmpty() &&
					!POSTagsToOutput.equalsIgnoreCase("null")) {
			partsOfSpeechToOutput = new HashMap<String,String>();				
			for (String posTag : POSTagsToOutput.split(" ")) {
				partsOfSpeechToOutput.put(posTag, posTag);
			}
			filterPOSTags = true;
		}
		// If this is the first time an instance of PartOfSpeechTag is instantiated,
		// read in the NLP model (which takes a second or so). 
		if (tagger == null) {
			try {
				tagger = new MaxentTagger(modelPath);
			} catch (ClassNotFoundException e) {
				logger.error("Part of speech tagger did not find model " + modelPath + ":" + e.getMessage());
			} catch (IOException e) {
				logger.error("Part of speech tagger did not find model " + modelPath + ":" + e.getMessage());
			} catch (Exception e) {
				logger.error("Cannot instantiate part of speech tagger: " + e.getMessage());
			}
		}
	}
	
	public Tuple exec(Tuple input) throws IOException {
		
    	String html = null;
    	TupleFactory mTupleFactory = TupleFactory.getInstance();
    	Tuple output = mTupleFactory.newTuple();
		
    	try {
    		if ((input.size() == 0) || 
    				((html = (String) input.get(0)) == null) ||
    				(html.length() == 0)) {
    			return null;
    		}

		} catch (ClassCastException e) {
			if (log.isWarnEnabled())
				log.warn("Part of speech tagger encountered a mal-formed input. Epected HTML string; called with: " + input);
			return null;
		}
		
		StringReader inputReader  = new StringReader(html);
		StringWriter outputWriter = new StringWriter(2 * html.length());
		
		try {
			// Do all the tagging, resulting in a string: "foo_NNP, bar_AD, ...". The 'false' 
			// parameter indicates that the tagger is not to read from stdin.
			tagger.runTagger(new BufferedReader(inputReader), new BufferedWriter(outputWriter), HTMLTagsToInclude, false);
		} catch (ClassNotFoundException e) {
			log.error("Part of speech tagging failed: " + e.getMessage());
			return null;
		} catch (NoSuchMethodException e) {
			log.error("Part of speech tagging failed: " + e.getMessage());
			return null;
		} catch (IllegalAccessException e) {
			log.error("Part of speech tagging failed: " + e.getMessage());
			return null;
		} catch (InvocationTargetException e) {
			log.error("Part of speech tagging failed: " + e.getMessage());
			return null;
		}
		
		String[] taggedTokens = outputWriter.toString().split(" ");
		if ((taggedTokens.length == 0) || (taggedTokens[0].length() == 0))
			return null;
		String word;
		String tag;
		for (String taggedToken : taggedTokens) {
			String[] wordAndTag = taggedToken.split("_");
			//*******
			if (wordAndTag.length < 2) {
				log.info("taggedToken: " + taggedToken);
				continue;
			}
			//*******
			word = wordAndTag[0];
			tag  = wordAndTag[1];
			// Filter stopwords, check for end of tagged words: ".",".",
			// If we are supposed to output only certain POS tags, check
			// whether the current tag qualifies:
			if (IsStopword.isStopword(word) || 
				word.endsWith(".") ||
				(filterPOSTags && ((tag = partsOfSpeechToOutput.get(tag)) == null)))
				continue;
			Tuple wordAndTagTuple = mTupleFactory.newTuple();
			wordAndTagTuple.append(word);
			wordAndTagTuple.append(tag);
			output.append(wordAndTagTuple);
		}
		// Last tag sometimes has a trailing newline. Take that out:
		Tuple lastTuple = (Tuple) output.get(output.size() - 1);
		lastTuple.set(1, ((String)(lastTuple.get(1))).trim());
		// The above might all be by reference, but why risk it?
		output.set(output.size() - 1, lastTuple);
		return output;
	}
	
	public Schema outputSchema(Schema input) {
		try {
			Schema wordTagSchema = new Schema();
			wordTagSchema.add(new Schema.FieldSchema("word", DataType.CHARARRAY));
			wordTagSchema.add(new Schema.FieldSchema("postag", DataType.CHARARRAY));

			// Outer schema of all word/tag tuples:
			Schema outSchema = new Schema(new Schema.FieldSchema("wordPosTags", wordTagSchema, DataType.TUPLE));
			return outSchema;
		} catch (Exception e){
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
