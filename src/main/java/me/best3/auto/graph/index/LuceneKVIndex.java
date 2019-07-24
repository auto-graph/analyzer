package me.best3.auto.graph.index;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class LuceneKVIndex extends LuceneIndex{
	private static final String KVINDEX = "./kvindex";
	private static final Logger logger = LogManager.getLogger(LuceneKVIndex.class);
	private static final String INDEX_LOCATION = "me.best3.auto.graph.index.LuceneIndexManager.LuceneKVIndex";
	private static final int READER_REFRESH_TIME = 400;
	
	LuceneKVIndex() throws IOException {
		super();
	}
	
	@Override
	public void write(String key, String value) throws IOException {
		logger.debug("write method called");
		Document doc = new Document();
		doc.add(new Field(key, value, TextField.TYPE_STORED));
		IndexWriter indexWriter = getIndexWriter();
		indexWriter.addDocument(doc);
		indexWriter.commit();
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
		IndexSearcher searcher = getSearchManager().acquire();
		try {
			WildcardQuery query = new WildcardQuery(new Term(key,"*"));
			TopDocs docs = searcher.search(query, 1);
			if (docs.totalHits.value > 0) {
				return  Optional.of(new SearchResult(searcher, docs.scoreDocs[0].doc));
			}
		}finally {
			getSearchManager().release(searcher);
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
		IndexSearcher searcher = getSearchManager().acquire();
		try{
			return searcher.getIndexReader().numDocs();
		}finally {
			getSearchManager().release(searcher);
		}
	}
	
	public void refreshReader() {
		if(logger.isDebugEnabled()) {
			logger.debug("Manual reader refresh.");
		}
		try {
			getSearchManager().maybeRefresh();
		} catch (IOException e) {
			logger.warn(e, e);
		}
	}
	
	public void debugDumpIndex() throws IOException {
		if(!logger.isDebugEnabled()) {
			return;
		}
		getSearchManager().maybeRefresh();
		IndexSearcher searcher = getSearchManager().acquire(); 
		try {
			IndexReader reader = searcher.getIndexReader();
			for(int i=0;i<reader.maxDoc();i++) {
				Document doc = reader.document(i);
				logger.debug(String.format("===========%d============",i));
				doc.getFields().stream().forEach(f ->{
					logger.debug(String.format("%s : %s", f.name(),f.stringValue()));
				});
				logger.debug("========================");
			}
			
		} catch(Exception e) {
			e.printStackTrace();
		}finally {
			getSearchManager().release(searcher);
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
