package me.best3.auto.graph.index;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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
	// Path where the index directory resides
	protected String indexLocation;
	private final UnicodeWhitespaceAnalyzer unicodeWhiteSpaceAnalyzer = new UnicodeWhitespaceAnalyzer();
	private Directory directory;
	private IndexWriter indexWriter;
	private SearcherManager searcherManager;
	
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

	private SearcherManager createReader() throws IOException {
		IndexWriter indexWriterRef = this.getIndexWriter();
		try {
			return new SearcherManager(indexWriterRef, new SearcherFactory());
		} catch (IndexNotFoundException e) {
			if(logger.isDebugEnabled()) {
				logger.warn(e,e);
			}
			write("00001","00001");//without this write searcher fails to find index on brand new instances of index
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
	
	public void write(String key, String value) throws IOException {
		logger.debug("write method called");
		Document doc = new Document();
		doc.add(new Field(key, value, TextField.TYPE_STORED));
		IndexWriter indexWriter = getIndexWriter();
		indexWriter.addDocument(doc);
//		indexWriter.commit();
	}
	
	public UnicodeWhitespaceAnalyzer getStandardAnalyzer() {
		return this.unicodeWhiteSpaceAnalyzer;
	}

	public Directory getDirectory() {
		return directory;
	}

	public IndexWriter getIndexWriter() {
		return this.indexWriter;
	}

	public SearcherManager getSearcherManager() throws IOException {
		if(!searcherManager.isSearcherCurrent()) {
			if(logger.isDebugEnabled()) {
				logger.debug("searcher not current.");
			}
			searcherManager.maybeRefreshBlocking();
		}
		return searcherManager;
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
		}
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
			public void beforeRefresh() throws IOException {
				logger.debug("Before refresh");
			}
			
			@Override
			public void afterRefresh(boolean didRefresh) throws IOException {
				logger.debug("after refresh " + didRefresh);
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

	private List<me.best3.auto.graph.index.Document> getUpToMaxDocs(IndexReader reader) throws IOException {
		List<me.best3.auto.graph.index.Document> documents = new ArrayList<me.best3.auto.graph.index.Document>();
		for(int i=0;i<reader.maxDoc();i++) {
			Document doc = reader.document(i);
			documents.add(new me.best3.auto.graph.index.Document(doc));
		}
		return documents;
	}	

	public void clear() {
		try {
			indexWriter.deleteAll();
//			indexWriter.flush();
//			indexWriter.commit();
		} catch (IOException e) {
			logger.debug(e,e);
		}
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
			multiCollectorManager.reduce(collectors);
			return getUpToMaxDocs(searcher.getIndexReader());
		}finally {
			searcherManager.release(searcher);
		}
		
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
	
	public String getIndexLocation() {
		return indexLocation;
	}

	/* Abstract methods*/
	protected abstract long getReaderRefreshTime();

}
