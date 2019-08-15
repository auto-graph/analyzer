package me.best3.auto.graph.index;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LuceneDocumentIndex extends LuceneIndex {
	
	private static final Logger logger = LogManager.getLogger(LuceneDocumentIndex.class);
	private static final int READER_REFRESH_TIME = 400;

	LuceneDocumentIndex(String indexLocation) throws IOException {
		super(indexLocation);
	}

	@Override
	protected long getReaderRefreshTime() {
		return READER_REFRESH_TIME;
	}
	
	public void write(Document document) throws IOException {
		if(logger.isDebugEnabled()) {
			logger.debug("Write called.");
		}
		getIndexWriter().addDocument(document.getDocument());
	}

	
	@Override
	protected Document getDocumentInstance(org.apache.lucene.document.Document doc) {
		return new DocumentWithID(doc);
	}
	
	
}
