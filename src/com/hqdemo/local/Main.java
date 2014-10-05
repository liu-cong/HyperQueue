package com.hqdemo.local;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Main {
	private static ThreadFactory factory=new ThreadManager();
	public static void main(String [] args){
		String topic1="topic1",msg1="messag1",topic2="topic2",msg2="message2";

		int poolsize=20;//size of the thread pool
		try{

			ExecutorService threadPool=Executors.newFixedThreadPool(poolsize,factory);
			for (int i=0;i<poolsize/4;i++)
			{
				threadPool.execute(new Producer("topic"+String.valueOf(i)," message"+String.valueOf(0)));
				threadPool.execute(new Producer("topic"+String.valueOf(i)," message"+String.valueOf(1)));

				threadPool.execute(new Consumer("topic"+String.valueOf(i)));
				threadPool.execute(new Consumer("topic"+String.valueOf(i+1)));
			}
			threadPool.shutdown();


			/*
			 * activate this block to test the broker in a sequential order
		    Producer producer1=new Producer(topic1,msg1);
		    Producer producer2=new Producer(topic2,msg2);
	      	Consumer consumer1=new Consumer(topic1);
		    Consumer consumer2=new Consumer(topic2);
			consumer1.getSessionID();//initialize the consumers
			consumer2.getSessionID();

			producer1.postMessage("topic1 msg1");
			producer1.postMessage("topic1 msg2");

			consumer1.getMessage();
			consumer1.getMessage();
			consumer1.getMessage();

			consumer2.getMessage();

			consumer2.setTopic("topic1");
			consumer2.getMessage();
			consumer2.getMessage();
			consumer2.getMessage();


			producer1.setTopic("topic2");
			producer1.postMessage("topic2 msg1");

			consumer1.getMessage();

			consumer1.setTopic("topic2");
			consumer1.getMessage();
			consumer1.getMessage();
			 */


		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

	public static class ThreadManager implements ThreadFactory{

		@Override
		public Thread newThread(Runnable r) {
			// TODO Auto-generated method stub
			return new Thread(r);
		}

	}
}
