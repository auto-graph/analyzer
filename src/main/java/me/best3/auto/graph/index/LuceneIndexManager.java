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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class LuceneIndexManager {
	private static final Logger logger = LogManager.getLogger(LuceneIndexManager.class);
	
	public class LuceneIndex{
		private static final int READER_REFRESH_TIME = 400;
		private static final String VALUE = "value";
		private static final String KEY = "key";
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
			doc.add(new Field(KEY, key, TextField.TYPE_STORED));
			doc.add(new Field(VALUE, value, TextField.TYPE_STORED));
			this.indexWriter.addDocument(doc);
			this.indexWriter.commit();
		}
		
		public String read(String key) throws IOException {
			logger.debug("read method called");
			QueryParser queryParser = new QueryParser(KEY, standardAnalyzer);
			IndexSearcher searcher = this.searchManager.acquire();
			try {
				Query query = queryParser.parse(key);
				TopDocs docs = searcher.search(query, 1);
				if (docs.totalHits.value > 0) {
					return searcher.doc(docs.scoreDocs[0].doc).get(VALUE);
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}finally {
				this.searchManager.release(searcher);
			}
			return null;
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
