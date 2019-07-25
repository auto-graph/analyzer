package me.best3.auto.graph.index;

import java.io.IOException;

import org.junit.jupiter.api.Test;

public class FileIndexerTest {
	
	private static final String FILE_INDEXER_TEST_JSON = "FileIndexerTest.json";

	@Test
	public void processTokens() throws IOException {
		FileIndexer fileIndexer = new FileIndexer();
		String testJsonFile = FileIndexerTest.class.getClassLoader().getResource(FILE_INDEXER_TEST_JSON).getFile();
		fileIndexer.processJSONFile(testJsonFile);
		fileIndexer.dumpIndex();
	}

}
