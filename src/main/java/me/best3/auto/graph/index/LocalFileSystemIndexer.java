package me.best3.auto.graph.index;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalFileSystemIndexer implements AutoCloseable{
	private static final Logger logger = LogManager.getLogger(LocalFileSystemIndexer.class); 
	
	private LuceneIndexFactory luceneIndexFactory = new LuceneIndexFactory();
	private LuceneDocumentIndex documentIndex ;
	
	public LocalFileSystemIndexer() throws IOException {
		this.documentIndex = luceneIndexFactory.getLuceneDocumentIndex();
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

	public void clear() {
		this.documentIndex.clear();
	}

	public String getIndexInformation() {
		return this.documentIndex.getIndexLocation();
	}

	
	@Override
	public void close() throws Exception {
		if(this.documentIndex!=null) {
			this.documentIndex.close();
		}
	}

	public long count(Document match) throws IOException {
		return documentIndex.count(match);
	}
}
