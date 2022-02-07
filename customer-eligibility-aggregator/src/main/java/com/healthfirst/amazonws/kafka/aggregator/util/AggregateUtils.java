package com.healthfirst.amazonws.kafka.aggregator.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.gson.JsonObject;

public class AggregateUtils {

	private static final Logger log = LogManager.getLogger(AggregateUtils.class);

	private String PrimaryKey = "hf_member_num_cd";
	
	public String aggregate(JsonObject json) {
		DynamoDBUtils dynamoDBUtils = new DynamoDBUtils();
		Item item = null;
		item = dynamoDBUtils.getItem(json.get(PrimaryKey).getAsString());
		if(item == null) {
			/**
			 * If the record doesn't exist in table, create new one to start with...
			 */
			item = new Item();
		}
		
		List<Item> itemList = new ArrayList<Item>();
		itemList.add(item);
		json.keySet().stream().forEach(key -> {
			log.info("Item key: " + key);
			Item listItem = itemList.get(0);
			listItem = listItem.withString(key, json.get(key).getAsString());
			itemList.set(0, listItem);
		});
		dynamoDBUtils.updateItem(itemList.get(0));
		
		return itemList.get(0).toJSON();
	}

}