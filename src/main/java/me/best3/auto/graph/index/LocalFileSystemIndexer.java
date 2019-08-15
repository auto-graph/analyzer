package me.best3.auto.graph.index;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalFileSystemIndexer extends FileIndexer {

	private static final Logger logger = LogManager.getLogger(LocalFileSystemIndexer.class);

	private LuceneIndexFactory luceneIndexFactory = new LuceneIndexFactory();
	private LuceneDocumentIndex documentIndex;
	private LuceneKVIndex kvIndex;
	private String indexLocation;

	public LocalFileSystemIndexer(String indexLocation) throws IOException {
		this.indexLocation = indexLocation;
		this.documentIndex = luceneIndexFactory.getLuceneDocumentIndex(this.indexLocation + "/docindex");
		this.kvIndex = luceneIndexFactory.getLuceneKVIndex(this.indexLocation + "/kvindex");
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

	public void dumpIndex() {
		try {
			this.documentIndex.debugDumpIndex();
		} catch (IOException e) {
			logger.error(e, e);
		}
	}

	public List<Document> getAllDocs() throws IOException {
		return documentIndex.getAllDocs();
	}

	public List<Document> getAllDocuments(Comparator<Document> comparator) throws IOException {
		List<Document> documents = this.getAllDocs();
		Collections.sort(documents, comparator);
		return documents;
	}

	public String getIndexLocation() {
		return indexLocation;
	}

	@Override
	public void writeDoc(Document document) throws IOException {
		List<Document> matches = this.documentIndex.exactMatches(document,document.getExcludeFields());
		if (matches.size() == 0) {
			if (logger.isDebugEnabled()) {
				logger.debug("Doc dosent exist attempting write.");
			}
			this.documentIndex.write(document);
		}
	}

	@Override
	public void writeKV(String key, String value) throws IOException {
		this.kvIndex.update(key, value);
	}

	
	@Override
	public Document getDocumentInstance() {
		return new DocumentWithID();
	}

}