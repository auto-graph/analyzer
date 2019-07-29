package me.best3.auto.graph.index;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ReferenceManager.RefreshListener;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

public abstract class LuceneIndex implements AutoCloseable{
	
	private static final Logger logger = LogManager.getLogger(LuceneIndex.class);
	// Path where the index directory resides
	protected String indexLocation;
	private final UnicodeWhitespaceAnalyzer unicodeWhiteSpaceAnalyzer = new UnicodeWhitespaceAnalyzer();
	private Directory directory;
	private IndexWriter indexWriter;
	private SearcherManager searcherManager;
	
	LuceneIndex() throws IOException {
		if(logger.isDebugEnabled())
		{
			logger.debug("Lucene index constructed.");
		}
		this.indexLocation = getIndexLocation();
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
					getIndexWriter().flush();
					getIndexWriter().commit();
					searcherManager.maybeRefresh();
					logger.debug("Timer attempting refresh");
				} catch (IOException e) {
					logger.debug(e,e);
				}
				
			}
		};
		
		Timer timer = new Timer(true);
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
		if(this.indexWriter!=null) {
			this.indexWriter.close();
		}
		if(this.searcherManager!=null) {
			this.searcherManager.close();
		}
		if(this.directory!=null) {
			this.directory.close();
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
			Query query = queryParser.parse("+fieldOne:fieldOne +fieldTwo:fieldTwo", "");
			
			int numHits = 100;
			TopDocs topDocs = searcher.search(query, numHits);
			logger.debug(String.format("total hists %s score docs : %s", topDocs.totalHits,topDocs.scoreDocs.length));
			do {
				for(ScoreDoc scoreDoc : topDocs.scoreDocs) {
					dumpDoc(searcher.doc(scoreDoc.doc));
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

	private void getUpToMaxDocs(IndexReader reader) throws IOException {
		for(int i=0;i<reader.maxDoc();i++) {
			Document doc = reader.document(i);
			dumpDoc(doc);
		}
	}

	private void dumpDoc(Document doc) throws IOException {
		logger.debug("========================");
		doc.getFields().stream().forEach(f ->{
			logger.debug(String.format("%s : %s", f.name(),f.stringValue()));
		});
		logger.debug("========================");
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
	/* Abstract methods*/
	public abstract String getIndexLocation();
	protected abstract long getReaderRefreshTime();
}
