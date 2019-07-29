package me.best3.auto.graph.index;

import java.io.IOException;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;

public class LuceneKVIndex extends LuceneIndex{
	private static final Logger logger = LogManager.getLogger(LuceneKVIndex.class);
	
	private static final String KVINDEX = "./kvindex";
	private static final String INDEX_LOCATION = "me.best3.auto.graph.index.LuceneKVIndex";
	private static final int READER_REFRESH_TIME = 400;
	
	LuceneKVIndex() throws IOException {
		super();
	}
	
	
	public String read(String key) throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("read method called");
		}
		Optional<SearchResult> searchReasult = findFirst(key);
		if(searchReasult.isPresent()) {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Read method found search result for %s", key));
			}
			return searchReasult.get().getDocument().get(key);
		}
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Read method did NOT find any result for %s", key));
		}
		return null;
	}
	
	public Optional<SearchResult> findFirst(String key) throws IOException {
		if (logger.isDebugEnabled()) {
			logger.debug("findFirst method called");
		}
		if(null==key) {
			throw new IOException("key cannot be null.");
		}
		
		//Query parser is not thread safe
		IndexSearcher searcher = getSearcherManager().acquire();
		try {
			WildcardQuery query = new WildcardQuery(new Term(key,"*"));
			TopDocs docs = searcher.search(query, 1);
			if (docs.totalHits.value > 0) {
				return  Optional.of(new SearchResult(searcher, docs.scoreDocs[0].doc));
			}
		}finally {
			getSearcherManager().release(searcher);
		}
		return Optional.empty();
	}
	
	/**
	 * Lucene is an index and not a cache, refrain from updates
	 * @param key
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public boolean update(String key, String value) throws IOException {
		WildcardQuery query = new WildcardQuery(new Term(key,"*"));
		getIndexWriter().deleteDocuments(query);
		write(key,value);
		return true;
	}
	
	public long getDocCount() throws IOException {
		IndexSearcher searcher = getSearcherManager().acquire();
		try{
			return searcher.getIndexReader().numDocs();
		}finally {
			getSearcherManager().release(searcher);
		}
	}
	
	public void refreshReader() {
		if(logger.isDebugEnabled()) {
			logger.debug("Manual reader refresh.");
		}
		try {
			getSearcherManager().maybeRefresh();
		} catch (IOException e) {
			logger.warn(e, e);
		}
	}
	
	@Override
	public String getIndexLocation() {
		return System.getProperty(INDEX_LOCATION, KVINDEX);
	}

	@Override
	protected long getReaderRefreshTime() {
		return  READER_REFRESH_TIME;
	}
}
