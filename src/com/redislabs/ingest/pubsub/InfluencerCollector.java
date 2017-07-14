package com.redislabs.ingest.pubsub;

import java.util.HashMap;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import redis.clients.jedis.Jedis;

/*
 * InfluencerCollector is a custom class that listens to Influencer Tweets,
 * stores their tweets and profile information in Redis data structures 
 */
public class InfluencerCollector extends Subscriber{

	// Redis connection to access Redis data structures
	private RedisConnection conn = null;
	private Jedis jedis = null;

	/*
	 * @param subscriberName: name of this object
	 * @param channel: name of the channel to listen to
	 */
	public InfluencerCollector(String subscriberName, String channel) throws Exception{
		super(subscriberName, channel);
		
		conn = RedisConnection.getRedisConnection();
		jedis = conn.getJedis();
	}
	
	/*
	 * Custom filter: get tweets, user profile; update Redis Hash and Sorted Set 
	 * 
	 * @see com.redislabs.ingest.pubsub.Subscriber#onMessage(java.lang.String, java.lang.String)
	 */
	@Override
	public void onMessage(String subscriberChannel, String message){	
		JsonParser jsonParser = new JsonParser();
		
		JsonElement jsonElement = jsonParser.parse(message);
		JsonObject jsonObject = jsonElement.getAsJsonObject();
		
		JsonObject userObject = jsonObject.get("user").getAsJsonObject();
		
		JsonElement followerCountElm = userObject.get("followers_count");
		try{
			if(followerCountElm != null && followerCountElm.getAsDouble() > 10000){
				String name = userObject.get("name").getAsString();
				String screenName = userObject.get("screen_name").getAsString();
				int followerCount = userObject.get("followers_count").getAsInt();
				int friendCount = userObject.get("friends_count").getAsInt();
							
				HashMap<String, String> map = new HashMap<String, String>();
				map.put("name", name);
				map.put("screen_name", screenName);
				if(userObject.get("location") != null){
					map.put("location", userObject.get("location").getAsString());				
				}
				map.put("followers_count", Integer.toString(followerCount));
				map.put("friendCount", Integer.toString(friendCount));

				jedis.zadd("influencers", followerCount, screenName);
				jedis.hmset("influencer:"+screenName, map);
				//System.out.println(userObject.get("screen_name").getAsString()+"| Followers:"+userObject.get("followers_count").getAsString());			
			}
			
		}catch(Exception e){
			System.out.println("ERROR: "+e.getMessage());
		}

	}
	
}