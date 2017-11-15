package com.rueggerllc.mongodb.tests;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class MongoTests {

	private static Logger logger = Logger.getLogger(MongoTests.class);
	
	@BeforeClass
	public static void setupClass() throws Exception {
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setupTest() throws Exception {
	}

	@After
	public void tearDownTest() throws Exception {
	}
	
	@Test
	@Ignore
	public void dummyTest() {
		logger.info("Inside Dummy Test");
	}
	
	@Test
	@Ignore
	public void testWriteDocument() {
		try {
			logger.info("Write Document Begin");
			
		} catch (Exception e) { 
			
		} finally {
			
		}
	}
	
	@Test
	@Ignore
	public void testWritePetCollection() {
		MongoClient mongoClient = null;
		try {
			logger.info("Write Pet Collection Begin");
			String[] nameList = {"Captain", "Oscar", "Darwin", "Sunny"};
			String[] speciesList = {"Canine", "Feline", "Avian", "Avian"};
			int[] age = {9,8,7,9};
			
			
			mongoClient = new MongoClient();
			logger.info("Connected to Mongo");
			
			MongoDatabase database = mongoClient.getDatabase("rueggerllc");
			MongoCollection<Document> petCollection = database.getCollection("petcollection");
			for (int i = 0; i < nameList.length; i++) {
				Document document = new Document();
				document.append("name",  nameList[i]);
				document.append("species",  speciesList[i]);
				document.append("age", age[i]);
				petCollection.insertOne(document);
				logger.info("Added: " + nameList[i]);
			}
			
		} catch (Exception e) { 
			
		} finally {
			close(mongoClient);
		}
		
	}
	
	
	@Test
	@Ignore
	public void testReadPetCollection() {
		MongoClient mongoClient = null;
		try {
			logger.info("Read Pet Collection Begin");
			mongoClient = new MongoClient();
			logger.info("Connected to Mongo");
			
			MongoDatabase database = mongoClient.getDatabase("rueggerllc");
			MongoCollection<Document> petCollection = database.getCollection("petcollection");
			
			MongoCursor<Document> cursor = petCollection.find().iterator();
			while (cursor.hasNext()) {
				logger.info("Next Pet=\n" + cursor.next().toJson());
			}
			close(cursor);
			
		} catch (Exception e) { 
			
		} finally {
			close(mongoClient);
		}
	}
	
	@Test
	// @Ignore
	public void testQueryCanines() {
		MongoClient mongoClient = null;
		try {
			logger.info("Read Pet Collection Begin");
			mongoClient = new MongoClient();
			logger.info("Connected to Mongo");
			
			MongoDatabase database = mongoClient.getDatabase("rueggerllc");
			MongoCollection<Document> petCollection = database.getCollection("petcollection");

			Bson filter = Filters.eq("species", "Canine");
			FindIterable<Document> results = petCollection.find(filter);
			for (Document document : results) {
				String name = document.getString("name");
				String species = document.getString("species");
				int age = document.getInteger("age", 0);
				String line = String.format("Name=%s Species=%s Age=%d", name, species, age);
				logger.info(line);
			}
			
		} catch (Exception e) { 
			
		} finally {
			close(mongoClient);
		}
		
	}
	
	
	
	
	// Utilties
	private void close(MongoClient mongoClient) {
		if (mongoClient != null) {
			mongoClient.close();
		}
	}
	
	private void close(MongoCursor<Document> cursor) {
		if (cursor != null) {
			cursor.close();
		}
	}
	
	
}
