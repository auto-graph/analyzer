package me.best3.auto.graph.index;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalFileSystemIndexer extends FileIndexer {
	private static final Logger logger = LogManager.getLogger(LocalFileSystemIndexer.class);

	private LuceneIndexFactory luceneIndexFactory = new LuceneIndexFactory();
	private LuceneDocumentIndex documentIndex;
	private String indexLocation;

	public LocalFileSystemIndexer(String indexLocation) throws IOException {
		this.indexLocation = indexLocation;
		this.documentIndex = luceneIndexFactory.getLuceneDocumentIndex(this.indexLocation);
	}

	@Override
	public void indexDocument(Document document) throws IOException {
		this.documentIndex.write(document);
	}

	public List<Document> getDocuments(Comparator<Document> comparator) throws IOException {
		List<Document> documents = this.getAllDocs();
		Collections.sort(documents, comparator);
		return documents;
	}

	public void dumpIndex() {
		try {
			this.documentIndex.debugDumpIndex();
		} catch (IOException e) {
			logger.error(e, e);
		}
	}

	public void clear() {
		this.documentIndex.clear();
	}

	@Override
	public void close() throws Exception {
		if (this.documentIndex != null) {
			this.documentIndex.close();
		}
	}

	public long count(Document match) throws IOException {
		return documentIndex.count(match);
	}

	public List<Document> getAllDocs() throws IOException {
		return documentIndex.getAllDocs();
	}

	
	public String getIndexLocation() {
		return indexLocation;
	}
	
}
