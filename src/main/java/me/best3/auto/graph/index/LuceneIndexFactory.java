package me.best3.auto.graph.index;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class LuceneIndexFactory {
	private static final Logger logger = LogManager.getLogger(LuceneIndexFactory.class);
	
	private static volatile LuceneKVIndex luceneKVIndex = null;

	public LuceneKVIndex getLuceneKVIndex() throws IOException {
		if(logger.isDebugEnabled()) {
			logger.debug("");
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

}
