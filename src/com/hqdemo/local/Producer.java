package com.hqdemo.local;

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
	private String url;
	private String query;
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
		
	}

	public void postMessage() {//fire HTTP POST method to post message to the server. to send a custom message, either call
		//setMessage(String) method or use postMessage(String) method
		HttpURLConnection con = null ;
		OutputStream output = null;
		InputStream input=null;	
		try {//establish HTTP connection and send message to the server
			con = (HttpURLConnection) ( new URL(url)).openConnection();
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			con.connect();	
			output = con.getOutputStream();
			output.write(query.getBytes(charset));//post message to the Broker
			input=con.getInputStream();
			System.out.println("[Producer status] A new message is posted. Topic="+topic+" Message="+message);
		}
		catch(Throwable t) {
			t.printStackTrace();
			System.out.println("[Producer status] Failed to post the message to the server.");
		}
		finally {
			try { input.close();output.close(); } catch(Throwable t) {}
			try { con.disconnect(); } catch(Throwable t) {}
		}

	}

	public void postMessage(String q) {//post message q to the server
		HttpURLConnection con = null ;
		OutputStream output = null;
		InputStream input=null;	
		try {
			con = (HttpURLConnection) ( new URL(url)).openConnection();
			con.setRequestMethod("POST");
			con.setDoOutput(true);
			con.connect();	
			output = con.getOutputStream();
			output.write(q.getBytes(charset));//post message to the Broker
			input=con.getInputStream();
		}
		catch(Throwable t) {
			t.printStackTrace();
			System.out.println("[Producer status] Failed to post the message to the server.");
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
