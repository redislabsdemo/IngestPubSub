package com.redislabs.ingest.pubsub;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/*
 * Suscriber class: Maintains Redis connection, subscribes to a channel, 
 * receives message inside onMessage method
 *  	
 */
class Subscriber extends JedisPubSub implements Runnable{
	private String name = "Subscriber";
	private RedisConnection conn = null;
	private Jedis jedis = null;
	
	private String subscriberChannel = "defaultchannel";
	
	/*
	 * @param subscriberName: Name of the subscriber
	 * @channelName: Name of the channel to subscribes to
	 */
	public Subscriber(String subscriberName, String channelName) throws Exception{
		name = subscriberName;
		subscriberChannel = channelName;
		
		Thread t = new Thread(this);
		t.start();
	}
	
	/*
	 * Logic to start the thread and listen to new messages.
	 * 
	 */
	@Override
	public void run(){
		try{
			conn = RedisConnection.getRedisConnection();
			jedis = conn.getJedis();
			System.out.println("Starting subscriber "+name);
			while(true){
				jedis.subscribe(this, this.subscriberChannel);
			}					
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
				
	
	/*
	 * @param channel: name of the channel the message is coming from
	 * @param message: the actual message
	 * @see redis.clients.jedis.JedisPubSub#onMessage(java.lang.String, java.lang.String)
	 */
	@Override
	public void onMessage(String channel, String message){
		super.onMessage(channel, message);
	}
		
}
