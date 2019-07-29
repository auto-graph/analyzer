package me.best3.auto.graph.index;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;

public class SearchResult {
	private int docId;
	private Document document;
	
	public SearchResult() {}

	public SearchResult(IndexSearcher searcher, int docId) throws IOException {
		super();
		this.docId = docId;
		this.document = searcher.doc(this.docId);
	}
	
	public Document getDocument() throws IOException {
		return this.document;
	}

	public int getDocId() {
		return docId;
	}

}
