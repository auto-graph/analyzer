package me.best3.auto.graph.index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FileIndexerTest {
	
	private static final String FILE_INDEXER_TEST_INDEX = "./fileIndexerTestIndex";
	private static final String FILE_INDEXER_TEST_JSON = "FileIndexerTest.json";
	private static LocalFileSystemIndexer localFSIndexer;
	private static String testJsonFile = FileIndexerTest.class.getClassLoader().getResource(FILE_INDEXER_TEST_JSON).getFile();
	
	@BeforeAll
	public static void setup() {
		try {
			FileIndexerTest.localFSIndexer = new LocalFileSystemIndexer(FILE_INDEXER_TEST_INDEX);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@AfterAll
	public static void tearDown() {
		if(FileIndexerTest.localFSIndexer!=null) {
			try {
				FileIndexerTest.localFSIndexer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void processTokens() throws IOException, InterruptedException {
		FileIndexerTest.localFSIndexer.processJSONFile(testJsonFile);
		Document doc = new Document();
		doc.addText("fieldOne", "fieldOne");
		doc.addText("fieldTwo", "fieldTwo");
		assertEquals(1,localFSIndexer.count(doc),"Document count mismatch");
	}

}
