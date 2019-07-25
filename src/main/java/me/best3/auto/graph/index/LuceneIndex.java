package me.best3.auto.graph.index;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public abstract class LuceneIndex implements AutoCloseable{
	
	private static final Logger logger = LogManager.getLogger(LuceneIndex.class);
	// Path where the index directory resides
	protected String indexLocation;
	private final StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
	private Directory directory;
	private IndexWriter indexWriter;
	private SearcherManager searchManager;
	
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
		this.directory = FSDirectory.open(indexPath);
		createWriter();
		createReader();
		refreshReaderTimer();
	}

	private void refreshReaderTimer() {
		TimerTask timerTask = new TimerTask() {
			
			@Override
			public void run() {
				try {
					searchManager.maybeRefresh();
					logger.debug("Timer attempting refresh");
				} catch (IOException e) {
					logger.debug(e,e);
				}
				
			}
		};
		
		Timer timer = new Timer(true);
		timer.scheduleAtFixedRate(timerTask, 0, getReaderRefreshTime());
	}

	private void createReader() throws IOException {
		try {
			this.searchManager = new SearcherManager(directory, new SearcherFactory());
		} catch (IndexNotFoundException e) {
			if(logger.isDebugEnabled()) {
				logger.warn(e,e);
			}
			write("00001","00001");//without this write searcher fails to find index on brand new instances of index
			this.indexWriter.deleteAll();
			this.indexWriter.commit();
			this.searchManager = new SearcherManager(directory, new SearcherFactory());
		}
	}

	private void createWriter() throws IOException {
		IndexWriterConfig indexWriterConfig = new IndexWriterConfig(this.standardAnalyzer);
		indexWriterConfig.setOpenMode(OpenMode.CREATE_OR_APPEND);
		this.indexWriter = new IndexWriter(directory, indexWriterConfig);
	}
	
	public void write(String key, String value) throws IOException {
		logger.debug("write method called");
		Document doc = new Document();
		doc.add(new Field(key, value, TextField.TYPE_STORED));
		IndexWriter indexWriter = getIndexWriter();
		indexWriter.addDocument(doc);
		indexWriter.commit();
	}
	
	public abstract String getIndexLocation();
	protected abstract long getReaderRefreshTime();

	public StandardAnalyzer getStandardAnalyzer() {
		return standardAnalyzer;
	}

	public Directory getDirectory() {
		return directory;
	}

	public IndexWriter getIndexWriter() {
		return this.indexWriter;
	}

	public SearcherManager getSearchManager() {
		return searchManager;
	}

	@Override
	public void close() throws Exception {
		if(this.indexWriter!=null) {
			this.indexWriter.close();
		}
		if(this.searchManager!=null) {
			this.searchManager.close();
		}
		if(this.directory!=null) {
			this.directory.close();
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

}
