package me.best3.auto.graph.index;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class LuceneIndexFactory {
	private static final Logger logger = LogManager.getLogger(LuceneIndexFactory.class);
	
	private static volatile LuceneKVIndex luceneKVIndex = null;
	private static volatile LuceneDocumentIndex luceneDocumentIndex = null;

	public LuceneKVIndex getLuceneKVIndex() throws IOException {
		if(logger.isDebugEnabled()) {
			logger.debug("get KV index");
		}
		if (LuceneIndexFactory.luceneKVIndex == null) {
			synchronized (this) {
				if (LuceneIndexFactory.luceneKVIndex == null) {
					LuceneIndexFactory.luceneKVIndex = new LuceneKVIndex();
				}
			}
		}
		return LuceneIndexFactory.luceneKVIndex;
	}
	
	public LuceneDocumentIndex getLuceneDocumentIndex() throws IOException {
		long startTime=0;
		boolean debugEnabled = logger.isDebugEnabled();
		if(debugEnabled) {
			logger.debug("get index");
			startTime = System.currentTimeMillis();
		}
		if (LuceneIndexFactory.luceneDocumentIndex == null) {
			synchronized (this) {
				if (LuceneIndexFactory.luceneDocumentIndex == null) {
					LuceneIndexFactory.luceneDocumentIndex = new LuceneDocumentIndex();
				}
			}
		}
		long endTime;
		if(debugEnabled) {
			endTime = System.currentTimeMillis();
			logger.debug(String.format("Time to return document index singleton instance is %d",(endTime-startTime)));
		}
		return LuceneIndexFactory.luceneDocumentIndex;
	}

}
