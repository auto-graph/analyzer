package me.best3.auto.graph.index;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalFileSystemIndexer {
	private static final Logger logger = LogManager.getLogger(LocalFileSystemIndexer.class); 
	
	private LuceneIndexFactory luceneIndexFactory = new LuceneIndexFactory();
	private LuceneDocumentIndex documentIndex ;
	
	public LocalFileSystemIndexer() throws IOException {
		this.documentIndex = luceneIndexFactory.getLuceneIndex();
	}
	
	public void indexDocument(Document document) throws IOException {
		this.documentIndex.write(document);
	}
	
	public void dumpIndex() {
		try {
			this.documentIndex.debugDumpIndex();
		} catch (IOException e) {
			logger.error(e,e);
		}
	}
}
