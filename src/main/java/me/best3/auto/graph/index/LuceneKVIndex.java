package me.best3.auto.graph.index;

import java.io.IOException;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;

public class LuceneKVIndex extends LuceneDocumentIndex{
	private static final String WILD_CARD = "*";

	private static final Logger logger = LogManager.getLogger(LuceneKVIndex.class);
	
	private static final int READER_REFRESH_TIME = 400;
	
	LuceneKVIndex(String indexLocation) throws IOException {
		super(indexLocation);
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
			Query query = getWildCardQuery(key);
			TopDocs docs = searcher.search(query, 1);
			if (docs.totalHits.value > 0) {
				return  Optional.of(new SearchResult(searcher, docs.scoreDocs[0].doc));
			}
		}finally {
			getSearcherManager().release(searcher);
		}
		return Optional.empty();
	}
	
	@Override
	protected long getReaderRefreshTime() {
		return  READER_REFRESH_TIME;
	}

	private WildcardQuery getWildCardQuery(String key) {
		return new WildcardQuery(new Term(key,WILD_CARD));
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
	
	/**
	 * Lucene is an index and not a cache, refrain from updates
	 * @param key
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public boolean update(String key, String value) throws IOException {
		Query query = getWildCardQuery(key);
		getIndexWriter().deleteDocuments(query);
		write(key,value);
		return true;
	}

	public void write(String key, String value) throws IOException {
		logger.debug("write method called");
		me.best3.auto.graph.index.Document doc = new me.best3.auto.graph.index.Document(new Document());
		doc.addString(key, value);
		super.write(doc);
	}
}
