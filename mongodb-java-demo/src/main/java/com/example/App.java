package com.example;

import org.bson.types.ObjectId;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.TransactionBody;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * CURD operation on Mongo DB
 * Agregation pipeline:
 */

public class App {

    private final static String DatabaseName = "first_db";
    private final static String CollectionName = "imaginedragons";

    public static void main(String[] args) {
        System.out.println("Hello World!");

        try (MongoClient mongoClient = MongoClientConnection.getMongoClient()) {

            MongoDatabase database = mongoClient.getDatabase(DatabaseName);
            MongoCollection<Document> collection = database.getCollection(CollectionName);
            System.out.println("Databases: ");
            mongoClient.listDatabaseNames().forEach(System.out::println);

            // ===================================================================================
            // Clean existing documents
            collection.deleteMany(Filters.eq("certificate_number", 9278806));
            collection.deleteMany(Filters.eq("account_id", "MDB99115881"));
            collection.deleteMany(Filters.eq("account_id", "MDB99115000"));
            collection.deleteMany(Filters.eq("account_id", "MDB310054629"));

            System.out.println("Cleaned existing documents/n");
            // ===================================================================================
            // CRUD operations
            // curdOperations(collection, mongoClient);

            // ===================================================================================
            // Aggergation operations
            aggerationPipeline(collection);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close the connection to MongoDB automatically
            System.out.println("Finally: closed the mongo db connection");
        }

    }

    private static void  aggerationPipeline(MongoCollection<Document> accounts) {
        // $math and $group example
        // Find an accpint belonging to _____, given an account number
        // fubd the average balance of all accounts
        
        //Seed data
        List<Document> docs = addPujaAccounts(5, accounts);
        //Match Stages
        Bson filter = Filters.eq("account_id", docs.get(0).get("account_id"));
        Bson matchStage = Aggregates.match(filter);
        System.out.println("\nMatch and Aggergate. Documents Matched: ");
        accounts.aggregate(Arrays.asList(matchStage)).forEach(document -> System.out.println(document.toJson()));
        
        //Group Stages ==ACCUMULATORS
        Bson groupStage = Aggregates.group("$account_type", 
                        Accumulators.sum("total_balance","$balance"), 
                        Accumulators.avg("average_balance", "$balance"));
        System.out.println("Displaying Aggregation Results: ");
        accounts.aggregate(Arrays.asList(matchStage, groupStage)).forEach(doc -> System.out.println(doc.toJson()));

        //===================================
        // $match, $sort, $project
        filter =Filters.and(
            Filters.eq("account_id", docs.get(0).get("account_id")),
            Filters.gt("balance", 1500));
        Bson matchStage2 = Aggregates.match(filter);
        
        Bson orderBy = Sorts.orderBy(Sorts.descending("balance"));
        Bson sortStage = Aggregates.sort(orderBy);

        Bson projectStage = Aggregates.project(
            Projections.fields(
                Projections.include("account_id", "account_type", "balance"),
                Projections.computed("euro_balance", new Document("$divide", Arrays.asList("$balance", 1.20F))),
                Projections.excludeId()));
        
        System.out.println("\nDisplaying match, order and project Results: ");
        accounts.aggregate(Arrays.asList(matchStage2, sortStage, projectStage)).forEach(doc -> System.out.println(doc.toJson()));
        
        //===================================)
    }

    
    private static List<Document> addPujaAccounts(int howMany, MongoCollection<Document> collection) {
        System.out.println("\nInserting many documents into the collection " );
        List<Document> docs = new ArrayList<Document>();              
        
        for (int i = 0; i < howMany; i++) {
            Document doc2 = new Document("_id", new ObjectId())
            .append("account_id", "MDB310054629")
            .append("account_holder", "Puja Barbier")
            .append("account_type", "savings")
            .append("balance", 1000 * i / 2);                         

            docs.add(doc2);

            doc2 = new Document("_id", new ObjectId())
            .append("account_id", "MDB310054629")
            .append("account_holder", "Puja Barbier")
            .append("account_type", "checkings")
            .append("balance", 3000 * i / 2);   

            docs.add(doc2);
        }

        if (docs.size() > 0) {
            InsertManyResult manyResult = collection.insertMany(docs);
            System.out.println("Inserted many documents into the collection: " + manyResult.getInsertedIds());
        }

        return docs;
    }

    private static void curdOperations(MongoCollection<Document> collection, MongoClient mongoClient){
        // ===================================================================================
            // Insert the document(s)
            Document doc = new Document("_id", new ObjectId())
                    .append("id", "10021-2015-ENFO")
                    .append("certificate_number", 9278806)
                    .append("busines_name", "John Doe")
                    .append("date",
                            Date.from(LocalDate.of(2015, 02, 20).atStartOfDay(ZoneId.systemDefault()).toInstant()))
                    .append("result", "No violation Issued")
                    .append("sector", "Cigarrette Retails Dealer - 127")
                    .append("address", new Document("street", "123 Main St")
                            .append("city", "Anytown")
                            .append("state", "CA")
                            .append("number", 12345));

            // Insert the ONE document into the collection
            InsertOneResult result = collection.insertOne(doc);
            System.out.println("Inserted a document into the collection. Id: " + result.getInsertedId());

            // Insert many documents into the collection
            Document[] docs = addAddcounts(3, collection);
            Document doc2 = docs[0];
            Document doc3 = docs[1];            

            // ===================================================================================
            // Find the documents by balance greater than 1000 and account_type equal to
            // "checking"
            // Create a filter for balance greater than 1000
            Bson balanceFilter = Filters.gt("balance", 1000);
            // Create a filter for account_type equal to "checking"
            Bson accountTypeFilter = Filters.eq("account_type", "checking");
            // assambly the query
            try (MongoCursor<Document> cursor = collection.find(Filters.and(balanceFilter, accountTypeFilter))
                    .cursor()) {
                System.out
                        .println("\nDocuments with balance greater than 1000 and account_type equal to \"checking\":");
                while (cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            }
            // ===================================================================================
            // Find the FIST document by balance greater than 1000 and account_type equal to
            // "checking"
            doc = collection.find(Filters.and(balanceFilter, accountTypeFilter)).first();
            System.out.println("\nFirst document: \n" + doc.toJson());
            // ===================================================================================
            // Update document
            Bson query = Filters.eq("account_id", doc.get("account_id"));
            Bson update = Updates.combine(
                    Updates.set("account_status", "active"),
                    Updates.inc("balance", 0.99));
            UpdateResult updateResult = collection.updateOne(query, update);
            System.out.println("\nUpdate result: " + updateResult.getModifiedCount());
            doc = collection.find(Filters.and(balanceFilter, accountTypeFilter)).first();
            System.out.println("\nAfter update document: \n" + doc.toJson());

            // ===================================================================================
            // Update document collections
            accountTypeFilter = Filters.eq("account_type", "savings");
            update = Updates.combine(Updates.set("balance", 10700 + System.currentTimeMillis()));
            updateResult = collection.updateMany(accountTypeFilter, update);
            System.out.println("\nUpdate MANY result: " + updateResult.getModifiedCount());
            try (MongoCursor<Document> crowler = collection.find(accountTypeFilter).cursor()) {
                System.out.println("\nSavings balance updated");
                while (crowler.hasNext()) {
                    System.out.println(crowler.next().toJson());
                }
            }

            // ===================================================================================
            // Delete document
            accountTypeFilter = Filters.eq("account_type", "savings");
            collection.deleteOne(accountTypeFilter);
            System.out.println("\nSavings balance deleted");
            try (MongoCursor<Document> cursor = collection.find(accountTypeFilter).cursor()) {
                System.out.println("\nSavings remaining");
                while (cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            }

            // ===================================================================================
            // Multi-document transaction: Tranfer $200 betwteen accounts
            final ClientSession session = mongoClient.startSession();
            TransactionBody<String> txBody = new TransactionBody<String>() {

                public String execute() {
                    Bson fromAccount = Filters.eq("account_id", doc2.get("account_id"));
                    Bson toAccount = Filters.eq("account_id", doc3.get("account_id"));
                    Bson updateFrom = Updates.combine(Updates.inc("balance", -200));
                    Bson updateTo = Updates.combine(Updates.inc("balance", 200));
                    collection.updateOne(fromAccount, updateFrom);
                    collection.updateOne(toAccount, updateTo);
                    return "Funds transferred successfully!";
                }// execute()

            };// TransactionBody
            try {
                String resp = session.withTransaction(txBody);
                System.out.println(resp);
            } catch (Exception e) {
                System.out.println("Transaction failed: " + e.getMessage());
                throw e;
            } finally {
                session.close();
            }

    }
    
    

    public static Document[] addAddcounts(int howMany, MongoCollection<Document> collection) {
        Document doc2 , doc3;
        doc2 = new Document("_id", new ObjectId())
        .append("account_id", "MDB99115881")
        .append("account_holder", "John Doe")
        .append("account_type", "checking")
        .append("balance", 1785);
        

        doc3 = new Document("_id", new ObjectId())
        .append("account_id", "MDB99115000")
        .append("account_holder", "Maria Doe")
        .append("account_type", "savings")
        .append("balance", 1468);
        
        for (int i = 0; i < howMany; i++) {
             doc2 = new Document("_id", new ObjectId())
                    .append("account_id", "MDB99115881")
                    .append("account_holder", "John Doe")
                    .append("account_type", "checking")
                    .append("balance", 1785);

             doc3 = new Document("_id", new ObjectId())
                    .append("account_id", "MDB99115000")
                    .append("account_holder", "Maria Doe")
                    .append("account_type", "savings")
                    .append("balance", 1468);

            List<Document> documents = Arrays.asList(doc2, doc3);

            InsertManyResult manyResult = collection.insertMany(documents);

            System.out.println("Inserted many documents into the collection: " + manyResult.getInsertedIds());
        }

        Document[] docs= {doc2, doc3};
        return docs;
    }
}
