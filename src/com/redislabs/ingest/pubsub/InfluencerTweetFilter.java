package com.redislabs.ingest.pubsub;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import redis.clients.jedis.Jedis;

/*
 * InfluencerFilter is a custom class that listens to a channel,
 * selects tweets from users who have more than 10,000 followers, 
 * and publishes them to another channel.
 * 
 */
public class InfluencerTweetFilter extends Subscriber
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
	public InfluencerTweetFilter(String name, String subscriberChannel, String publisherChannel) throws Exception{
		super(name, subscriberChannel); // initialize the subscriber
		this.publisherChannel = publisherChannel; // initialize the publisher
		conn = RedisConnection.getRedisConnection();
		jedis = conn.getJedis();				
	}
	
	/*
	 * Filters a tweet based on the follower count of the user
	 * @see com.redislabs.ingest.pubsub.Subscriber#onMessage(java.lang.String, java.lang.String)
	 */
	@Override
	public void onMessage(String subscriberChannel, String message){	
		JsonParser jsonParser = new JsonParser();
		
		JsonElement jsonElement = jsonParser.parse(message);
		JsonObject jsonObject = jsonElement.getAsJsonObject();
		
		JsonObject userObject = jsonObject.get("user").getAsJsonObject();
		
		JsonElement followerCountElm = userObject.get("followers_count");
		if(followerCountElm != null && followerCountElm.getAsDouble() > 10000){
			//System.out.println(userObject.get("screen_name").getAsString()+"| Followers:"+userObject.get("followers_count").getAsString());			
			//System.out.println(jsonObject.get("text").getAsString());
			
			// publish the message if the follower count is greater than 10000
			jedis.publish(publisherChannel, message);
		}
	}
	
}