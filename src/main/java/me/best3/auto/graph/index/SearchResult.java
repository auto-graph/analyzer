package me.best3.auto.graph.index;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;

public class SearchResult {
	private IndexSearcher searcher;
	private int docId;
	
	public SearchResult() {}

	public SearchResult(IndexSearcher searcher, int docId) {
		super();
		this.searcher = searcher;
		this.docId = docId;
	}
	
	public Document getDocument() throws IOException {
		return this.searcher.doc(this.docId);
	}

}
