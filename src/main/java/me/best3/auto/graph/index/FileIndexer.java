package me.best3.auto.graph.index;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import me.best3.auto.graph.analyzer.SubsetComparator;

/**
 * Nested documents are flattened out
 *
 */
public class FileIndexer implements AutoCloseable{
	private static final Logger logger = LogManager.getLogger(FileIndexer.class);
	private LocalFileSystemIndexer localFSIndexer;
	private JsonFactory jsonFactory = new JsonFactory();

	public FileIndexer() throws IOException {
		boolean isDebugEnabled = logger.isDebugEnabled();
		long t1 = 0;
		if(isDebugEnabled) {
			t1 = System.currentTimeMillis();
		}
		
		this.localFSIndexer = new LocalFileSystemIndexer();
		
		long t2 = 0;
		if(isDebugEnabled) {
			t2 = System.currentTimeMillis();
			logger.debug(String.format("File Indexer time %d", (t2-t1)));
		}
	}

	public void processJSONFile(String fileName) {
		try(JsonParser jsonParser = jsonFactory.createParser(new File(fileName))) {
			processTockens(jsonParser);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	//logger.info(String.format("%s : %s : %s", jsonParser.currentToken(), jsonParser.currentName(), jsonParser.getCurrentValue()));
	private void processTockens(JsonParser jsonParser) {
		Stream.generate(getTokenSupplier(jsonParser))
			.takeWhile(t -> t != null)
			//TDOD: Debug statement remove later
			.peek(t -> logCurrentToken("processTockens",jsonParser))
//			.forEach(t -> logCurrentToken("processTockens",jsonParser));
			.forEach(t ->processToken(jsonParser));
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
	 * A valid JSON starts to be an object or an array
	 * @throws IOException 
	 * */
	public void processToken(JsonParser jsonParser){
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
	
	public void processToken(JsonParser jsonParser,Document document) {
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
	
	private void processArray(JsonParser jsonParser) {
		logCurrentToken("processArray",jsonParser);
		Stream.generate(getTokenSupplier(jsonParser))
		.takeWhile(t -> (t != null && !t.equals(JsonToken.END_ARRAY)) )
		.forEach(t -> processToken(jsonParser));
	}

	private void processObject(JsonParser jsonParser) throws IOException {
		Document document = new Document();
		Stream.generate(getTokenSupplier(jsonParser))
		.takeWhile(t -> (t != null && !t.equals(JsonToken.END_OBJECT)) )
		.forEach(t -> processToken(jsonParser,document));
		this.localFSIndexer.indexDocument(document);
	}

	private void processField(JsonParser jsonParser, Document document) {
		logCurrentToken("processField",jsonParser);
		try {
			document.addString(jsonParser.currentName(), jsonParser.currentName());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

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
	
	void dumpIndex() {
		this.localFSIndexer.dumpIndex();
	}
	
	public long count(Document match) throws IOException {
		return localFSIndexer.count(match);
	}
	
	public void clear() {
		if(logger.isDebugEnabled()) {
			logger.debug(String.format("Deleting docs from %s", this.localFSIndexer.getIndexInformation()));
		}
		this.localFSIndexer.clear();
	}

	
	@Override
	public void close() throws Exception{
		if(this.localFSIndexer!=null) {
			this.localFSIndexer.close();
		}
	}

	
	public List<Document> getDocuments(Comparator<Document> comparator) throws IOException {
		List<Document> documents = localFSIndexer.getAllDocs();
		Collections.sort(documents, comparator);
		return documents;
	}

}
