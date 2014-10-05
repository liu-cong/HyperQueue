package com.iWellSite.test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class Producer implements Runnable{
	private String baseURL="http://hqdemo.nat123.net";
	private String topic="newUser";
	private String message="hello world";
	private String url="http://localhost:8080//HyperQueue//Broker";
	private String query="topic:newUser&message=helloworld";
	private String charset = "UTF-8";
	
	public Producer(String topic, String message){
		this.topic=topic;
		this.message=message;
		this.buildUrl();
		this.buildQuery();
	}
	@Override
	public void run() {
		postMessage();//use HTTP POST method to post message to the Broker
		System.out.println("Producer status: topic="+topic+" Message="+message+"\r\n");
	}
	
	public void postMessage() {
		HttpURLConnection con = null ;
		OutputStream output = null;
		InputStream input=null;	
		try {
			con = (HttpURLConnection) ( new URL(url)).openConnection();
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			con.connect();	
			output = con.getOutputStream();
			output.write(query.getBytes(charset));//post message to the Broker
            input=con.getInputStream();
	    }
		catch(Throwable t) {
			t.printStackTrace();
			System.out.println("Failed to post message on the server.");
		}
		finally {
			try { input.close();output.close(); } catch(Throwable t) {}
			try { con.disconnect(); } catch(Throwable t) {}
		}
		
	}

	private void buildQuery(){query="topic="+topic+"&message="+message;}
	
	private void buildUrl(){url=baseURL+"/HyperQueue/Broker";}
	
	public void setTopic(String topic){this.topic=topic;this.buildQuery();}
	
	public void setMessage(String message){this.message=message;this.buildQuery();}
}
