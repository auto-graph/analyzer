package me.best3.auto.graph.index;

import java.io.IOException;

public class LuceneDocumentIndex extends LuceneIndex {
	
	private static final String KVINDEX = "./docindex";
	private static final String INDEX_LOCATION = "me.best3.auto.graph.index.LuceneJSONIndex";
	private static final int READER_REFRESH_TIME = 400;

	LuceneDocumentIndex() throws IOException {
		super();
	}

	@Override
	public String getIndexLocation() {
		return System.getProperty(INDEX_LOCATION, KVINDEX);
	}

	@Override
	protected long getReaderRefreshTime() {
		return READER_REFRESH_TIME;
	}
	
	public void write(Document document) throws IOException {
		getIndexWriter().addDocument(document.getDocument());
		getIndexWriter().commit();
	}

}
