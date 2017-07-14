package com.redislabs.ingest.pubsub;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import redis.clients.jedis.Jedis;


/*
 * EnglishTweetFilter is a custom class that listens to a channel,
 * selects tweets that are marked lang=en, and publishes them 
 * to another channel.
 */
public class EnglishTweetFilter extends Subscriber
{
	
	// connection, Jedis object, and publisherChannel 
	// used to publish the filtered tweets.
	private RedisConnection conn = null;
	private Jedis jedis = null;	
	private String publisherChannel = null;
	
	/*
	 * @param name: Name of this filter object
	 * @param subscriberChannel: Listen to this channel
	 * @param publisherChannel: Publish to this channel
	 */
	public EnglishTweetFilter(String name, String subscriberChannel, String publisherChannel) throws Exception{
		super(name, subscriberChannel); // initialize the subscriber
		this.publisherChannel = publisherChannel; // initialize the publisher
		conn = RedisConnection.getRedisConnection();
		jedis = conn.getJedis();		
	}
	
	/*
	 * Filters the message based on lang=en  
	 * @see com.redislabs.ingest.pubsub.Subscriber#onMessage(java.lang.String, java.lang.String)
	 */
	@Override
	public void onMessage(String subscriberChannel, String message){	
		JsonParser jsonParser = new JsonParser();
		
		JsonElement jsonElement = jsonParser.parse(message);
		JsonObject jsonObject = jsonElement.getAsJsonObject();
		
		if(jsonObject.get("lang") != null && jsonObject.get("lang").getAsString().equals("en")){
			//System.out.println(jsonObject.get("text").getAsString());
			
			//publish the message only if the filtering conditions are true 
			jedis.publish(publisherChannel, message);
		}
	}
	
}