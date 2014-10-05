package com.iWellSite.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class Main {
	private static ThreadFactory factory=new ThreadManager();
	public static void main(String [] args){
		String topic1="topic1",msg1="messag1",topic2="topic2",msg2="message2";
		Producer producer1=new Producer(topic1,msg1);
		Producer producer2=new Producer(topic2,msg2);
		Consumer consumer1=new Consumer(topic1);
		Consumer consumer2=new Consumer(topic2);
		try{
			ExecutorService threadPool=Executors.newFixedThreadPool(10,factory);
			threadPool.execute(producer1);
			//threadPool.execute(producer2);
			threadPool.execute(consumer1);
			//threadPool.execute(consumer2);
			
			producer1.setMessage("new message1");
			threadPool.execute(producer1);
			
			consumer1.setTopic(topic2);
			threadPool.execute(consumer1);
			
			//producer2.setMessage("new message2");
			//threadPool.execute(producer2);
			
			//threadPool.execute(consumer2);
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
