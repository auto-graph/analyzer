package me.best3.auto.graph.index;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class LuceneIndexFactory {
	private static final Logger logger = LogManager.getLogger(LuceneIndexFactory.class);
	
	private static final Map<String, LuceneKVIndex> luceneKVIndexCache = Collections.synchronizedMap(new HashMap<String,LuceneKVIndex>());
	private static final Map<String, LuceneDocumentIndex> luceneDocumentIndexCache = Collections.synchronizedMap(new HashMap<String,LuceneDocumentIndex>());

	public LuceneDocumentIndex getLuceneDocumentIndex(String indexLocation) throws IOException {
		long startTime=0;
		boolean debugEnabled = logger.isDebugEnabled();
		if(debugEnabled) {
			logger.debug("get index");
			startTime = System.currentTimeMillis();
		}
		if (!LuceneIndexFactory.luceneDocumentIndexCache.containsKey(indexLocation) || !LuceneIndexFactory.luceneDocumentIndexCache.get(indexLocation).isOpen()) {
			synchronized (LuceneIndexFactory.luceneDocumentIndexCache) {
				if (!LuceneIndexFactory.luceneDocumentIndexCache.containsKey(indexLocation)  || !LuceneIndexFactory.luceneDocumentIndexCache.get(indexLocation).isOpen()) {
					LuceneIndexFactory.luceneDocumentIndexCache.put(indexLocation, new LuceneDocumentIndex(indexLocation));
				}
			}
		}
		long endTime;
		if(debugEnabled) {
			endTime = System.currentTimeMillis();
			logger.debug(String.format("Time to return document index singleton instance is %d",(endTime-startTime)));
		}
		return LuceneIndexFactory.luceneDocumentIndexCache.get(indexLocation);
	}	

	public LuceneKVIndex getLuceneKVIndex(String indexLocation) throws IOException {
		if(logger.isDebugEnabled()) {
			logger.debug("get KV index");
		}
		if (!LuceneIndexFactory.luceneKVIndexCache.containsKey(indexLocation) || !LuceneIndexFactory.luceneKVIndexCache.get(indexLocation).isOpen()) {
			synchronized (LuceneIndexFactory.luceneKVIndexCache) {
				if (!LuceneIndexFactory.luceneKVIndexCache.containsKey(indexLocation) || !LuceneIndexFactory.luceneKVIndexCache.get(indexLocation).isOpen()) {
					LuceneIndexFactory.luceneKVIndexCache.put(indexLocation,new LuceneKVIndex(indexLocation));
				}
			}
		}
		return LuceneIndexFactory.luceneKVIndexCache.get(indexLocation);
	}
	
}
