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
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;


public class LuceneIndexManager {
	private static final Logger logger = LogManager.getLogger(LuceneIndexManager.class);
	
	public class LuceneIndex{
		private static final int READER_REFRESH_TIME = 400;
		// Path where the index directory resides
		private String indexLocation;
		private final StandardAnalyzer standardAnalyzer = new StandardAnalyzer();
		private Directory directory;
		private IndexWriter indexWriter;
		private SearcherManager searchManager;
		
		private LuceneIndex() throws IOException {
			if(logger.isDebugEnabled())
			{
				logger.debug("Lucene index constructed.");
			}
			this.indexLocation = System.getProperty("INDEX_LOCATION", "./index");
			Path indexPath = Paths.get(indexLocation);
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
						logger.debug("may be refresh");
					} catch (IOException e) {
						logger.debug(e,e);
					}
					
				}
			};
			
			Timer timer = new Timer(true);
			timer.scheduleAtFixedRate(timerTask, 0, READER_REFRESH_TIME);
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
			this.indexWriter.addDocument(doc);
			this.indexWriter.commit();
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
			IndexSearcher searcher = this.searchManager.acquire();
			try {
				WildcardQuery query = new WildcardQuery(new Term(key,"*"));
				TopDocs docs = searcher.search(query, 1);
				if (docs.totalHits.value > 0) {
					return  Optional.of(new SearchResult(searcher, docs.scoreDocs[0].doc));
				}
			}finally {
				this.searchManager.release(searcher);
			}
			return Optional.empty();
		}
		
		public boolean update(String key, String value) throws IOException {
			TermQuery query = new TermQuery(new Term(key));
			this.indexWriter.deleteDocuments(query);
			write(key,value);
			return true;
		}
		
		public void debugDumpIndex() throws IOException {
			if(!logger.isDebugEnabled()) {
				return;
			}
			this.searchManager.maybeRefresh();
			IndexSearcher searcher = this.searchManager.acquire(); 
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
				this.searchManager.release(searcher);
			}
		}
	}
	
	private static volatile LuceneIndex luceneIndex = null;

	public LuceneIndex getLuceneIndexManager() throws IOException {
		if (LuceneIndexManager.luceneIndex == null) {
			synchronized (this) {
				if (LuceneIndexManager.luceneIndex == null) {
					LuceneIndexManager.luceneIndex = new LuceneIndex();
				}
			}
		}
		return LuceneIndexManager.luceneIndex;
	}



}
