package me.best3.auto.graph.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Date;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

public class LuceneIndexManagerTest {
	
	private static final String KVINDEX = "./kvindex";
	private static LuceneKVIndex index;
	
	@BeforeAll
	public static void setUp() throws Exception {
		LuceneIndexManagerTest.index = new LuceneIndexFactory().getLuceneKVIndex(KVINDEX);
	}
	

	@AfterAll
	public  static void tearDown() throws Exception {
		LuceneIndexManagerTest.index.close();
	}

	@RepeatedTest(3)
	public void testGetLuceneIndexManager() {
		try {
			assertEquals( new LuceneIndexFactory().getLuceneKVIndex(KVINDEX).hashCode(), LuceneIndexManagerTest.index.hashCode(),"Singleton working . ");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@RepeatedTest(3)
	public void testUpdate() {
		try {
			//pay the price for refreshing the reader, its OK as this is just a test 
//			LuceneIndexManagerTest.index.refreshReader();
			long docCount = LuceneIndexManagerTest.index.getDocCount();
			//the very first time we know we expect 2 documents,
			//but index is not aware of the incoming doc write that update does
			if(LuceneIndexManagerTest.index.read("WriteTest") == null) {
				docCount++;
			}
			LuceneIndexManagerTest.index.update("WriteTest", "WriteTest-updated");
			//pay the price for refreshing the reader, its OK as this is just a test 
//			LuceneIndexManagerTest.index.refreshReader();
			long newDocCount =  LuceneIndexManagerTest.index.getDocCount();
			//since update deletes and adds new doc, and doc count 
			//returns all documents including deleted documents
			assertEquals(docCount, newDocCount,"Document count different");
			
			
			assertEquals(LuceneIndexManagerTest.index.read("WriteTest"),"WriteTest-updated","Updated value matched");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	

	@RepeatedTest(3)
	public void testWrite() {
		try {
			LuceneIndexManagerTest.index.write("Test", "Test value " + new Date());
			
			//Pay the price for refreshing the reader since this is a test
//			LuceneIndexManagerTest.index.refreshReader();
			
			//read it back
			String value = LuceneIndexManagerTest.index.read("Test");
			assertNotNull(value,"Read value is null ");
			if(value!=null) {
				assertTrue(value.startsWith("Test value"),"Read operation working");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

}
