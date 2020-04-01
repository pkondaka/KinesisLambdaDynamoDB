package com.cloudvave.awslambda.kinesis.handlers;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent.KinesisEventRecord;

public class ProcessKinesisRecords implements RequestHandler<KinesisEvent, Void>{
  @Override
  public Void handleRequest(KinesisEvent event, Context context)
  {
	  PutItemOutcome outcome = null;
	  int counter = 0;

	  System.out.println("Record Size - " + event.getRecords().size());

	  // Create a connection to DynamoDB
      AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
      DynamoDB dynamoDB = new DynamoDB(client);

      // Get a reference to the Widget table
      Table table = dynamoDB.getTable("STOCKS_DATA");



    for(KinesisEventRecord rec : event.getRecords()) {

    	//skip if first, do work for rest
        if(counter>0){
            System.out.println("Seq Num = " + new String(rec.getKinesis().getSequenceNumber()));
            System.out.println("Data Array = " + new String(rec.getKinesis().getData().array()));

            String[] values = new String(rec.getKinesis().getData().array()).split(",");

            outcome = table.putItem(new Item()
							.withPrimaryKey("Symbol",values[0])
							.withString("Name", (values[1].isEmpty()? "NO_DATA" : values[1]))
							.withString("Sector", (values[2].isEmpty()? "NO_DATA" : values[2]))
							.withString("Price", (values[3].isEmpty()? "NO_DATA" : values[3]))
							.withString("Price/Earnings", (values[4].isEmpty()? "NO_DATA" : values[4]))
							.withString("Dividend Yield", (values[5].isEmpty()? "NO_DATA" : values[5]))
							.withString("Earnings/Share", (values[6].isEmpty()? "NO_DATA" : values[6]))
							.withString("52 Week Low", (values[7].isEmpty()? "NO_DATA" : values[7]))
							.withString("52 Week High", (values[8].isEmpty()? "NO_DATA" : values[8]))
							.withString("Market Cap", (values[9].isEmpty()? "NO_DATA" : values[9]))
							.withString("EBITDA", (values[10].isEmpty()? "NO_DATA" : values[10]))
							.withString("Price/Sales", (values[11].isEmpty()? "NO_DATA" : values[11]))
							.withString("Price/Book", (values[12].isEmpty()? "NO_DATA" : values[12]))
							.withString("SEC Filings", (values[13].isEmpty()? "NO_DATA" : values[13]))
      			  );
            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        }
        counter++;
    }
    return null;


  }
}