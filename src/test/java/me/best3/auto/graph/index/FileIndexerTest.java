package me.best3.auto.graph.index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class FileIndexerTest {
	
	private static final String FILE_INDEXER_TEST_JSON = "FileIndexerTest.json";
	private static FileIndexer fileIndexer;
	private static String testJsonFile = FileIndexerTest.class.getClassLoader().getResource(FILE_INDEXER_TEST_JSON).getFile();
	
	@BeforeAll
	public static void setup() {
		try {
			fileIndexer = new FileIndexer();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@AfterAll
	public static void tearDown() {
		if(FileIndexerTest.fileIndexer!=null) {
			try {
				FileIndexerTest.fileIndexer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void processTokens() throws IOException, InterruptedException {
		fileIndexer.processJSONFile(testJsonFile);
		Document doc = new Document();
		doc.addText("fieldOne", "fieldOne");
		doc.addText("fieldTwo", "fieldTwo");
		assertEquals(1,fileIndexer.count(doc),"Document count mismatch");
	}

}
