package me.best3.auto.graph.index;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.UnicodeWhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.MultiCollectorManager.Collectors;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager.RefreshListener;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

public abstract class LuceneIndex implements AutoCloseable{
	
	private static final Logger logger = LogManager.getLogger(LuceneIndex.class);
	
	public static final String INDEX_LOCATION_PROPERTY_SUFIX = ".indexLocation";
	private static final int DOC_LIMIT =100;
	// Path where the index directory resides
	protected String indexLocation;
	private final UnicodeWhitespaceAnalyzer unicodeWhiteSpaceAnalyzer = new UnicodeWhitespaceAnalyzer();
	private Directory directory;
	private IndexWriter indexWriter;
	private SearcherManager searcherManager;
	private boolean closed = false;
	
 	LuceneIndex(String indexLocation) throws IOException {
		if(logger.isDebugEnabled())
		{
			logger.debug("Lucene index constructed.");
		}
		this.indexLocation = indexLocation;
		Path indexPath = Paths.get(indexLocation);
		if(logger.isDebugEnabled())
		{
			logger.debug(String.format("Intializing index at %s.",indexPath));
		}
		this.directory = NIOFSDirectory.open(indexPath);
		createWriter();
		this.searcherManager = createReader();
		refreshReaderTimer();
	}

	public void clear() {
		try {
			indexWriter.deleteAll();
		} catch (IOException e) {
			logger.debug(e,e);
		}
	}

	@Override
	public void close() throws Exception {
		try {
			if(this.indexWriter!=null) {
					this.indexWriter.close();
			}
			if(this.searcherManager!=null) {
				this.searcherManager.close();
			}
			if(this.directory!=null) {
				this.directory.close();
			}
		}catch(IllegalStateException e) {
			logger.debug(e);
		}finally {
			this.closed = true;
		}
	}
	
	public boolean isOpen() {
		return !this.closed;
	}

	private void collectExactMatches(me.best3.auto.graph.index.Document match, IndexSearcher searcher, 
			TopDocs topDocs, List<me.best3.auto.graph.index.Document> results, String...excludeField) {
		results.addAll(Arrays.asList(topDocs.scoreDocs).parallelStream()
				.map(scoreDoc -> {
			try {
				return new me.best3.auto.graph.index.Document(searcher.doc(scoreDoc.doc));
			} catch (IOException e) {
				logger.debug(e,e);
			}
			return new me.best3.auto.graph.index.Document();
		})
				.filter(d -> {
					List<String> resultFields = d.getFields();
					boolean excludeFieldMismatch = false;
					for(String field : resultFields) {
						if(!resultFields.contains(field)) {
							excludeFieldMismatch = true;
							break;
						}
					}
					return !excludeFieldMismatch && //all fields we excluded should be present
							match.getFields().size() == resultFields.size()-excludeField.length; // the only length difference should the excluded fields
					}) // there are exactly the same fields we want no more or less
				.collect(java.util.stream.Collectors.toList()));
	}
	
	private void collectResults(IndexSearcher searcher, TopDocs topDocs,
			List<me.best3.auto.graph.index.Document> results) {
		results.addAll(Arrays.asList(topDocs.scoreDocs).parallelStream().map(scoreDoc -> {
			try {
				return new me.best3.auto.graph.index.Document(searcher.doc(scoreDoc.doc));
			} catch (IOException e) {
				logger.debug(e,e);
			}
			return new me.best3.auto.graph.index.Document();
		}).collect(java.util.stream.Collectors.toList()));
	}
	
	public int count(me.best3.auto.graph.index.Document match) throws IOException {		
		SearcherManager searcherManager = getSearcherManager();
		IndexSearcher searcher = searcherManager.acquire();
		try {
			return searcher.count(match.getAllFieldsMatchQuery());
		}finally {
			searcherManager.release(searcher);
		}
	}

	private SearcherManager createReader() throws IOException {
		IndexWriter indexWriterRef = this.getIndexWriter();
		try {
			return new SearcherManager(indexWriterRef, new SearcherFactory());
		} catch (IndexNotFoundException e) {
			if(logger.isDebugEnabled()) {
				logger.warn(e,e);
			}
			internalWrite("00001","00001");//without this write searcher fails to find index on brand new instances of index
			this.indexWriter.deleteAll();
			this.indexWriter.commit();
			return new SearcherManager(indexWriterRef, new SearcherFactory());
		}
	}

	private void createWriter() throws IOException {
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(this.unicodeWhiteSpaceAnalyzer);
		if(logger.isDebugEnabled()) {
			logger.debug(String.format("reader attributes %s", indexWriterConfig.getReaderAttributes()));
		}
		indexWriterConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
		this.indexWriter = new IndexWriter(directory, indexWriterConfig);
	}

	public void debugDumpIndex() throws IOException {
		if(!logger.isDebugEnabled()) {
			return;
		}
		IndexSearcher searcher = getSearcherManager().acquire();
		boolean refreshStatus = getSearcherManager().maybeRefresh();
		if(logger.isDebugEnabled()) {
			logger.debug(String.format("Refresh attempt returned %s", refreshStatus));
		}
		getSearcherManager().addListener(new RefreshListener() {
			
			@Override
			public void afterRefresh(boolean didRefresh) throws IOException {
				logger.debug("after refresh " + didRefresh);
			}
			
			@Override
			public void beforeRefresh() throws IOException {
				logger.debug("Before refresh");
			}
		});
		try {
			IndexReader reader = searcher.getIndexReader();
			if(logger.isDebugEnabled()) {
				logger.debug(String.format("Maxdoc count in debugDump call is %d", reader.maxDoc()));
			}
			
//			getUpToMaxDocs(reader);
			StandardQueryParser queryParser = new StandardQueryParser();
			Query query = queryParser.parse("*:*", "");
			
			int numHits = 100;
			TopDocs topDocs = searcher.search(query, numHits);
			logger.debug(String.format("total hists %s score docs : %s", topDocs.totalHits,topDocs.scoreDocs.length));
			do {
				for(ScoreDoc scoreDoc : topDocs.scoreDocs) {
					new me.best3.auto.graph.index.Document(searcher.doc(scoreDoc.doc)).toJSON();
				}
				if(topDocs.scoreDocs.length>0) {
					topDocs = searcher.searchAfter(topDocs.scoreDocs[topDocs.scoreDocs.length-1], query, numHits);
				}
			}while(topDocs.scoreDocs.length>0);
		} catch(Exception e) {
			e.printStackTrace();
		}finally {
			getSearcherManager().release(searcher);
		}
	}

	public List<me.best3.auto.graph.index.Document> exactMatches(me.best3.auto.graph.index.Document match,String...excludeField) throws IOException {
		SearcherManager searcherManager = getSearcherManager();
		IndexSearcher searcher = searcherManager.acquire();
		List<me.best3.auto.graph.index.Document> results = new ArrayList<me.best3.auto.graph.index.Document>();
		try {
			TopDocs topDocs = search(match, DOC_LIMIT,excludeField);
			collectExactMatches(match, searcher, topDocs, results, excludeField);
			while(topDocs.scoreDocs.length>=DOC_LIMIT) {
				topDocs = search(topDocs.scoreDocs[topDocs.scoreDocs.length-1], match.getAllFieldsMatchQuery(Arrays.asList(excludeField)), DOC_LIMIT);
				collectExactMatches(match, searcher, topDocs, results, excludeField);
			}
		}finally {
			searcherManager.release(searcher);
		}
		return results;
	}
	
	/**
	 * At least all the fields in match document will be present on result. 
	 * There may be other fields on the matched document. This method will not do an exact match
	 * 
	 * @param match
	 * @return
	 */
	public boolean exists(me.best3.auto.graph.index.Document match,String...excludeField) {
		try {
			boolean existanceCheck = (findFirst(match,excludeField)!=null);
			if(logger.isDebugEnabled()) {
				logger.debug(String.format("existence check returned %s",existanceCheck) );
			}
			return existanceCheck;
		} catch (IOException e) {
			logger.warn(e,e);
			return false;
		}
	}

	public List<me.best3.auto.graph.index.Document> find(me.best3.auto.graph.index.Document match,String...excludeField) throws IOException{
		SearcherManager searcherManager = getSearcherManager();
		IndexSearcher searcher = searcherManager.acquire();
		List<me.best3.auto.graph.index.Document> results = new ArrayList<me.best3.auto.graph.index.Document>();
		try {
			TopDocs topDocs = search(match, DOC_LIMIT,excludeField);
			collectResults(searcher, topDocs, results);
			while(topDocs.scoreDocs.length>=DOC_LIMIT) {
				topDocs = search(topDocs.scoreDocs[topDocs.scoreDocs.length-1], match.getAllFieldsMatchQuery(Arrays.asList(excludeField)), DOC_LIMIT);
				collectResults(searcher, topDocs, results);
			}
		}finally {
			searcherManager.release(searcher);
		}
		return results;
	}	

	public me.best3.auto.graph.index.Document findFirst(me.best3.auto.graph.index.Document match,String...excludeField) throws IOException {
		if(logger.isDebugEnabled()) {
			logger.debug("find first using document : " + match.toJSON());
		}
		TopDocs topDocs = search(match,1,excludeField);
		if(logger.isDebugEnabled()) {
			logger.debug("topDocs count : " + topDocs.totalHits.value);
		}
		if(topDocs.totalHits.value>0) {
			return getDocument(topDocs.scoreDocs[0].doc);
		}
		return null;
	}
	
	public List<me.best3.auto.graph.index.Document> getAllDocs() throws IOException {
		SearcherManager searcherManager = getSearcherManager(); 
		IndexSearcher searcher = searcherManager.acquire();
		try {
			return getUpToMaxDocs(searcher.getIndexReader());
		}finally {
			searcherManager.release(searcher);
		}
		
	}
	
	public List<me.best3.auto.graph.index.Document> getAllDocs(Query query) throws IOException {
		SearcherManager searcherManager = getSearcherManager(); 
		IndexSearcher searcher = searcherManager.acquire();
		try {
			MultiCollectorManager multiCollectorManager = new MultiCollectorManager();
			Collectors collector = multiCollectorManager.newCollector();
			searcher.search(query, collector);
			ArrayList<MultiCollectorManager.Collectors> collectors = new ArrayList<MultiCollectorManager.Collectors>();
			collectors.add(collector);
			Object[] docs = multiCollectorManager.reduce(collectors);
			return getUpToMaxDocs(searcher.getIndexReader());
		}finally {
			searcherManager.release(searcher);
		}
		
	}
	
	public Directory getDirectory() {
		return directory;
	}
	
	public long getDocCount() throws IOException {
		IndexSearcher searcher = getSearcherManager().acquire();
		try{
			return searcher.getIndexReader().numDocs();
		}finally {
			getSearcherManager().release(searcher);
		}
	}

	private me.best3.auto.graph.index.Document getDocument(int docID) throws IOException {
		SearcherManager searcherManager = getSearcherManager();
		IndexSearcher searcher = searcherManager.acquire();
		try {
			return new me.best3.auto.graph.index.Document(searcher.doc(docID));
		}finally {
			searcherManager.release(searcher);
		}
	}
	
	public String getIndexLocation() {
		return indexLocation;
	}
	
	public IndexWriter getIndexWriter() {
		return this.indexWriter;
	}
	
	/* Abstract methods*/
	protected abstract long getReaderRefreshTime();

	public SearcherManager getSearcherManager() throws IOException {
		if(!searcherManager.isSearcherCurrent()) {
			if(logger.isDebugEnabled()) {
				logger.debug("searcher not current.");
			}
			searcherManager.maybeRefreshBlocking();
		}
		return searcherManager;
	}
	
	public UnicodeWhitespaceAnalyzer getStandardAnalyzer() {
		return this.unicodeWhiteSpaceAnalyzer;
	}

	private List<me.best3.auto.graph.index.Document> getUpToMaxDocs(IndexReader reader) throws IOException {
		List<me.best3.auto.graph.index.Document> documents = new ArrayList<me.best3.auto.graph.index.Document>();
		for(int i=0;i<reader.maxDoc();i++) {
			Document doc = reader.document(i);
			documents.add(getDocumentInstance(doc));
		}
		return documents;
	}
	
	protected me.best3.auto.graph.index.Document getDocumentInstance(Document doc) {
		return new me.best3.auto.graph.index.Document(doc);
	}

	private void internalWrite(String key, String value) throws IOException {
		logger.debug("Internal write method called");
		Document doc = new Document();
		doc.add(new Field(key, value, TextField.TYPE_STORED));
		IndexWriter indexWriter = getIndexWriter();
		indexWriter.addDocument(doc);
	}
	
	private void refreshReaderTimer() {
		TimerTask timerTask = new TimerTask() {
			
			@Override
			public void run() {
				try {
					if (logger.isDebugEnabled()) {
						logger.debug(String.format("Timer for index %s attempting flush,commit and refresh reader",indexLocation));
					}
					if(getIndexWriter().isOpen()) {
						getIndexWriter().flush();
						getIndexWriter().commit();
						searcherManager.maybeRefresh();
					}else {
						this.cancel();
					}
				}catch(	AlreadyClosedException |
						IOException e) {
					logger.debug(e);
				}
				
			}
		};
		
		Timer timer = new Timer("Timer-"+indexLocation,true);
		timer.scheduleAtFixedRate(timerTask, 0, getReaderRefreshTime());
	}
	
	public TopDocs search(me.best3.auto.graph.index.Document match, int topN,String...excludeField) throws IOException {
		if(match!=null && topN>0) {
			Query query = match.getAllFieldsMatchQuery(Arrays.asList(excludeField));
			if(query!=null) {//not a possible scenario just being defensive
				return search(query, topN);
			}
		}
		return null;
	}
	
	protected final TopDocs search(Query query, int topN) throws IOException {
		SearcherManager searcherManager = getSearcherManager();
		IndexSearcher searcher = searcherManager.acquire();
		try {
			if(logger.isDebugEnabled()) {
				logger.debug("searching using query : " + query.toString());
			}
			if(query!=null) {
				return searcher.search(query, topN);
			}
		}finally {
			searcherManager.release(searcher);
		}
		return null;
	}
	
	public final TopDocs search(ScoreDoc doc,Query query, int topN) throws IOException {
		SearcherManager searcherManager = getSearcherManager();
		IndexSearcher searcher = searcherManager.acquire();
		try {
			if(logger.isDebugEnabled()) {
				logger.debug("searching using query : " + query.toString());
			}
			if(query!=null) {
				return searcher.searchAfter(doc,query, topN);
			}
		}finally {
			searcherManager.release(searcher);
		}
		return null;
	}

}
