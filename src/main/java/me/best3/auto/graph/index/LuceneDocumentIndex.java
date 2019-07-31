package me.best3.auto.graph.index;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;

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
		if(!exists(document)) {
			if(logger.isDebugEnabled()) {
				logger.debug("Doc dosent exist attempting write.");
			}
			getIndexWriter().addDocument(document.getDocument());
		}
	}
	
	public Document findFirst(Document match) throws IOException {
		if(logger.isDebugEnabled()) {
			logger.debug("find first using document : " + match.toJSON());
		}
		TopDocs topDocs = search(match,1);
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
	
	private final TopDocs search(Document match, int topN) throws IOException {
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
			Document existingDoc =findFirst(document);
			boolean existanceCheck = (
					existingDoc!= null && // at least a doc exists with all the fields 
//					new SubsetComparator().compare(existingDoc, document)==0 &&  
					existingDoc.getFields().size() == document.getFields().size() // no other fields other than what we searched for, if a superset is found then we need
					);
			if(logger.isDebugEnabled()) {
				logger.debug(String.format("existence check returned %s",existanceCheck) );
			}
			return existanceCheck;
		} catch (IOException e) {
			logger.warn(e,e);
			return false;
		}
		
	}


}
