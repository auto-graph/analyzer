package me.best3.auto.graph.index;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;

public class LuceneDocumentIndex extends LuceneIndex {
	
	public static final Logger logger = LogManager.getLogger(LuceneDocumentIndex.class);
	
	private static final String KVINDEX = "./docindex";
	private static final String INDEX_LOCATION = "me.best3.auto.graph.index.LuceneJSONIndex";
	private static final int READER_REFRESH_TIME = 400;

	LuceneDocumentIndex() throws IOException {
		super();
	}

	@Override
	public String getIndexLocation() {
		return System.getProperty(INDEX_LOCATION, KVINDEX);
	}

	@Override
	protected long getReaderRefreshTime() {
		return READER_REFRESH_TIME;
	}
	
	public void write(Document document) throws IOException {
		if(logger.isDebugEnabled()) {
			logger.debug("Write called.");
		}
		if(!exists(document)) {
			if(logger.isDebugEnabled()) {
				logger.debug("Doc dosent exist attempting write.");
			}
			getIndexWriter().addDocument(document.getDocument());
//			getIndexWriter().flush();
//			getIndexWriter().commit();
		}
	}
	
	public Document findFirst(Document match) throws IOException {
		if(logger.isDebugEnabled()) {
			logger.debug("find first using document : " + match);
		}
		TopDocs topDocs = search(match);
		if(logger.isDebugEnabled()) {
			logger.debug("topDocs count : " + topDocs.totalHits.value);
		}
		if(topDocs.totalHits.value>0) {
			return getDocument(topDocs.scoreDocs[0].doc);
		}
		return null;
	}

	private Document getDocument(int docID) throws IOException {
		SearcherManager searcherManager = getSearcherManager();
		IndexSearcher searcher = searcherManager.acquire();
		try {
			return new Document(searcher.doc(docID));
		}finally {
			searcherManager.release(searcher);
		}
	}
	
	private final TopDocs search(Document match) throws IOException {
		SearcherManager searcherManager = getSearcherManager();
		IndexSearcher searcher = searcherManager.acquire();
		try {
			Query query = match.getAllFieldsMatchQuery();
			
			if(logger.isDebugEnabled()) {
				logger.debug("searching using query : " + query.toString());
			}
			if(query!=null) {
				return searcher.search(query, 1);				
			}
		}finally {
			searcherManager.release(searcher);
		}
		return null;
	}
	
	public boolean exists(Document document) {
		try {
			return findFirst(document)!= null;
		} catch (IOException e) {
			logger.warn(e,e);
			return false;
		}
		
	}

	public long count(Document match) throws IOException {		
		return search(match).totalHits.value;
	}

}
