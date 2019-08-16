package me.best3.auto.graph.index;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Nested documents are flattened out
 * This indexer is aware of processing a JSON file and delegating the work of indexing to concrete classes 
 *
 */
public abstract class JSONFileIndexer implements AutoCloseable{
	private static final Logger logger = LogManager.getLogger(JSONFileIndexer.class);
	private JsonFactory jsonFactory = new JsonFactory();

	public abstract void clear();

	@Override
	public abstract void close() throws Exception;

	public abstract List<Document> getAllDocuments(Comparator<Document> comparator) throws IOException;
	
	//walks through the tokens
	private Supplier<JsonToken> getTokenSupplier(JsonParser jsonParser) {
		return () -> {
			try {
				logger.debug("Move to next token");
				return jsonParser.nextToken();
			} catch (IOException e) {
				logger.error(e, e);
			}
			return null;
		};
	}
	
	private void logCurrentToken(String caller, JsonParser jsonParser) {
		try {
			if(logger.isDebugEnabled()) {
				logger.debug(String.format("%s => %s %s %s", caller, jsonParser.currentToken(), jsonParser.currentName(), jsonParser.getValueAsString()));
			}
		} catch (IOException e) {
			logger.error(e,e);
		}
	}

	/**
	 * Called at an array boundary, this method process that array in its entirety
	 * 
	 * @param jsonParser
	 */
	private void processArray(JsonParser jsonParser) {
		logCurrentToken("processArray",jsonParser);
		Stream.generate(getTokenSupplier(jsonParser))
		.takeWhile(t -> (t != null && !t.equals(JsonToken.END_ARRAY)) )
		.forEach(t -> processToken(jsonParser));
	}

	/**
	 * Captures a fields name
	 * 
	 * @param jsonParser
	 * @param document
	 */
	private void processField(JsonParser jsonParser, Document document) {
		logCurrentToken("processField",jsonParser);
		try {
			String fieldName = jsonParser.currentName().trim();
			if(fieldName.length()>0) {
				document.addString(fieldName, fieldName);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Parses JSON and generates a token stream and processes all tokens
	 * @param fileName
	 */
	public void processJSONFile(String fileName) {
		try(JsonParser jsonParser = jsonFactory.createParser(new File(fileName))) {
			Stream.generate(getTokenSupplier(jsonParser))
			.takeWhile(t -> t != null)
			//TDOD: Debug statement remove later
			.peek(t -> logCurrentToken("processTockens",jsonParser))
			.forEach(t ->processToken(jsonParser));
		} catch (IOException e) {
			logger.debug(e,e);
		}
	}
	
	/**
	 * This is called at beginning of an object boundary and process that 
	 * object in its entirety.
	 * Creates a new instance of document and builds the document
	 * 
	 * @param jsonParser
	 * @throws IOException
	 */
	private void processObject(JsonParser jsonParser) throws IOException {
		Document document = getDocumentInstance();
		Stream.generate(getTokenSupplier(jsonParser))
		.takeWhile(t -> (t != null && !t.equals(JsonToken.END_OBJECT)) )
		.forEach(t -> processToken(jsonParser,document));
		if (document.getFields().size()>0) {//Drop empty objects
			this.writeDoc(document);
		}
	}
	
	/**
	 * A valid JSON always starts with an object or an array
	 * @throws IOException 
	 * */
	private void processToken(JsonParser jsonParser){
		logCurrentToken("processToken",jsonParser);
		switch(jsonParser.currentToken()) {
		case START_ARRAY:
			processArray(jsonParser);
			break;
		case START_OBJECT:
			try {
				processObject(jsonParser);
			} catch (IOException e) {
				logger.error(e,e);
			}
			break;		
		default:
			logCurrentToken("dropping from processTokenSwitch",jsonParser);
			break;
		}
	}
	
	private void processToken(JsonParser jsonParser,Document document) {
		logCurrentToken("processTokenDoc",jsonParser);
		switch(jsonParser.currentToken()) {
		case START_OBJECT:
			try {
				processObject(jsonParser);
			} catch (IOException e) {
				logger.error(e,e);
			}
			break;
		case FIELD_NAME:
			processField(jsonParser,document);
			break;
		default://VALUE_STRING will be dropped
			logCurrentToken("dropping from processTocketSwitchDocument",jsonParser);
			break; 
		}
	}
	
	//Abstract methods
	/**
	 * @param document
	 * @param excludeField exclude these fields from duplicate check when upserting
	 * @throws IOException
	 */
	public abstract void writeDoc(Document document) throws IOException;
	public abstract void writeKV(String key, String value) throws IOException;
	
	/**
	 * Return a new instance of document to be populated with Data
	 * @return
	 */
	public abstract Document getDocumentInstance();

}
