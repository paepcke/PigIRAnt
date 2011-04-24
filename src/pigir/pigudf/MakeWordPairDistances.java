package pigir.pigudf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class MakeWordPairDistances extends EvalFunc<Tuple>  {
	
	/**
	 * Given a tuple of index entries, such as they are produced
	 * by buildWebBaseIndex, produce a flattened-out co-occurrence matrix.
	 * The index entries are of the form (word,docID,wordPosition):
	 * Two examples:
	 * 
	 * Input : ({(man,d1,0),(lives,d1,1),(sun,d1,2)})
     * would generate:
     *    (man,lives,1,d1)
     *    (man,sun,2,d1)
     *    (lives,sun,1,d1)
     *    
	 * Input: ({(Girl,d2,0),(child,d2,1),(lives,d2,2),(close,d2,3)})
     * would generate:
     * ((girl,child,1,d2
     *	(child,lives,1,d2
     *	(girl,lives,2,d2
     *	(lives,close,1,d2
     *	(child,close,2,d2
     *	(girl,close,3,d2))
     *
     * The maximum distance between words that will still output
     * these distance tuples is set in MAX_DISTANCE_TO_RECORD.
     * Note that all words will be converted to lower case in the
     * output.
     * 
     * Output schema is one tuple with 4-tuples inside: 
     *   {distances: (word1: chararray,word2: chararray,distance: int,docID: chararray)}
     *
	 * @author paepcke
	 *
	 */
	
	final int MAX_DISTANCE_TO_RECORD = 5;
		
	final int IN_WORD_INDEX = 0;
	final int IN_DOCID_INDEX = 1;
	final int IN_POSITION_INDEX = 2;
	
	final int OUT_WORD1_INDEX = 0;
	final int OUT_WORD2_INDEX = 1;
	final int OUT_DISTANCE_INDEX = 2;
	final int OUT_DOCID_INDEX = 3;
	
	final byte ASCII_CAP_A = 65;
	final byte ASCII_CAP_Z = 90;
	final byte ASCII_LOW_A = 97;
	final byte ASCII_CAP_TO_LOW_OFFSET = ASCII_LOW_A - ASCII_CAP_A; 
	
	// Word must be an instance variable, so that fastLowerCaseWord()
	// can get to its bytes (rather than a copy being passed to 
	// a method):
	String word = null;
		
	public Tuple exec(Tuple input) throws IOException {
		
		Log log = getLogger();
		
		DataBag inputWordDocIDDistanceBag = null;
		
		// We expect one element in each tuple:
		// bag of word+docID+distance tuples.
		try {
			if (input == null || 
					input.size() < 1)
				return null;
			if ((inputWordDocIDDistanceBag = (DataBag) input.get(0)) == null)
				return null;
			// No meaningful word distance w/o at least two words: 
			if (inputWordDocIDDistanceBag.size() < 2) 
				return null;
		} catch (ClassCastException e) {
			if (log.isWarnEnabled())
				log.warn("MakeWordPairDistances expect a bag of word/docID/wordPosition tuples, but got the mal-formed: " + input);
			return null;
		}
		
		TupleFactory tFact = TupleFactory.getInstance();
		Tuple distancesTuple = tFact.newTuple();
		
		// ({(Girl,d2,0),(child,d2,1),(lives,d2,2),(close,d2,3)})
		Iterator<Tuple> tripletIt = inputWordDocIDDistanceBag.iterator();
		// We know from the input check that we have at
		// least two word/docID/wordPos triplets; get the first:
		Tuple oneTriplet = tripletIt.next();
		int numTriplets  = (int) inputWordDocIDDistanceBag.size();
		String words[]   = new String[numTriplets];
		int positions[]  = new int[numTriplets];
		String docID = (String) oneTriplet.get(IN_DOCID_INDEX);
		word = (String) oneTriplet.get(IN_WORD_INDEX);
		fastLowerCaseWord();
		words[0] = word;
		positions[0] = (Integer) oneTriplet.get(IN_POSITION_INDEX);
		
		int inputIndex = 1;
		int position;
		while (tripletIt.hasNext()) {
			oneTriplet = tripletIt.next();
			word = ((String) oneTriplet.get(IN_WORD_INDEX));
			// Lower-case word in place:
			fastLowerCaseWord();
			position = (Integer) oneTriplet.get(IN_POSITION_INDEX);
			words[inputIndex] = word;
			positions[inputIndex] = position;
			int distance;
			// Generate all distance result tuples, going 
			// backward in the words and positions arrays:
			for (int i=inputIndex-1; i>-1; i--) {
				// We only register words that are <= MAX_DISTANCE_TO_RECORD
				// from the current word: 
				distance = Math.abs(position - positions[i]);
				if (distance > MAX_DISTANCE_TO_RECORD)
					continue;
				Tuple t = tFact.newTuple(4);
				t.set(OUT_WORD1_INDEX, words[i]);
				t.set(OUT_WORD2_INDEX, word);
				t.set(OUT_DISTANCE_INDEX, distance);
				t.set(OUT_DOCID_INDEX, docID);
				distancesTuple.append(t);
			}
			inputIndex++;
		}
		return distancesTuple;
	}
	
	/* 
	 * Output schema is {(w1,w2,dist,docID),(w1,w2,dist,docID),...}.
	 * 
	 * (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#outputSchema(org.apache.pig.impl.logicalLayer.schema.Schema)
	 */
	public Schema outputSchema(Schema input) {
		Schema resultSchema = null;
		try {
			Schema distanceSchema = new Schema();
			distanceSchema.add(new Schema.FieldSchema("word1",DataType.CHARARRAY));
			distanceSchema.add(new Schema.FieldSchema("word2",DataType.CHARARRAY));
			distanceSchema.add(new Schema.FieldSchema("distance",DataType.INTEGER));
			distanceSchema.add(new Schema.FieldSchema("docID",DataType.CHARARRAY));

			// Wrap these into a tuple:
			resultSchema = new Schema();
			resultSchema.add(new Schema.FieldSchema("distances", distanceSchema, DataType.TUPLE));

		} catch (FrontendException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return resultSchema;
	}
	
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FieldSchema> fields = new ArrayList<FieldSchema>(3);
		//fields.add(new FieldSchema(null, DataType.INTEGER));
		//fields.add(new FieldSchema(null, DataType.INTEGER));
		fields.add(new FieldSchema(null, DataType.BAG));
		FuncSpec funcSpec = new FuncSpec(this.getClass().getName(), new Schema(fields));
		List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>(1);
		funcSpecs.add(funcSpec);
		return funcSpecs;
	}
	
	/**
	 * If the first char of instance variable 'word' begins
	 * with an ASCII-8 upper case char, replace that first
	 * char with its lower case equivalent.
	 * Hopefully this is faster than something like
	 *   word.substring(0,1).toUpperCase() + word.substring(1);
	 * Since strings are immutable, we still create a new string
	 * object here.
	 * 
	 * NOTE: this will not work with unicode.
	 */
	public void fastLowerCaseWord() {
		byte[] wordBytes = word.getBytes();
		byte firstCharByte = wordBytes[0];
		if (firstCharByte < ASCII_CAP_A || firstCharByte > ASCII_CAP_Z)
			// Word doesn't start with a cap letter; nothing to be done:
			return;
		wordBytes[0] = (byte) (firstCharByte + ASCII_CAP_TO_LOW_OFFSET);
		word = new String(wordBytes);
	}
	
	/**
	 * Setter/Getter for instance var 'word'. Only available so that 
	 * unit tests can ensure proper functioning of fastLowerCaseWord();
	 * @param newWord
	 */
	public void setWord(String newWord) {
		word = newWord;
	}
	public String getWord() {
		return word;
	}
}

