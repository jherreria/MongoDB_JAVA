package com.example;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MongoClientConnection {
  private static MongoClient mongoClient;

  public static synchronized MongoClient getMongoClient() throws MongoException {
    if (mongoClient == null) {
      Properties props = getConfiguration();
      String password = props.getProperty("db.pwd");
      String connectionString = props.getProperty("db.connectionString").replace("<password>", password);
       

      ServerApi serverApi = ServerApi.builder()
          .version(ServerApiVersion.V1)
          .build();

      MongoClientSettings settings = MongoClientSettings.builder()
          .applyConnectionString(new ConnectionString(connectionString))
          .serverApi(serverApi)
          .build();

      // Create a new client and connect to the server
      mongoClient = MongoClients.create(settings);
      
      // Send a ping to confirm a successful connection
      MongoDatabase database = mongoClient.getDatabase("admin");
      database.runCommand(new Document("ping", 1));
      System.out.println("Pinged your deployment. You successfully connected to MongoDB!");
      
    }
    
    return mongoClient;
  }

  private static Properties getConfiguration(){
    String pathToConfig = Thread.currentThread().getContextClassLoader().getResource("resources/config.properties").getPath();    
    File  configFile=new File(pathToConfig);    
    try(InputStream input = new FileInputStream(configFile)) {
      Properties props = new Properties();
      props.load(input);
      System.out.println("\nConfigurationProps: " + props + "\n");
      return props;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } 
  }
}
