package com.hqdemo.local;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class Consumer implements Runnable{
	private String baseURL="http://hqdemo.nat123.net";
	private String url;
	private String sessionID=null;
	private String topic;
	private String query;

	public Consumer(String topic){this.topic=topic;this.buildUrl();this.buildQuery();}
	@Override
	public void run() {
		if(sessionID==null)//if this is the first time the Consumer connects the server, ask a session ID from the server
			getSessionID();

		String msg=null;
		if(sessionID!=null)
			msg=getMessage();//get message from the server regarding topic

		
	}

	public void getSessionID(){
		HttpURLConnection con = null ;
		InputStream is = null;

		try {//fire HTTP GET request and read response (session ID) from the server
			con = (HttpURLConnection) ( new URL(url)).openConnection();
			con.setRequestMethod("GET");
			con.setDoInput(true);
			con.connect();

			StringBuffer buffer = new StringBuffer();
			is = con.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while (  (line = br.readLine()) != null )
				buffer.append(line);
			String id=buffer.toString();
			System.out.println("[Consumer status] new session ID is obtained. ID="+id+".");

			this.setSessionID(buffer.toString());
		}
		catch(Throwable t) {
			t.printStackTrace();
			System.out.println("[Consumer status] Failed to get session ID.");
		}
		finally {
			try { is.close(); } catch(Throwable t) {}
			try { con.disconnect(); } catch(Throwable t) {}
		}
	}



	public String getMessage() {//fire HTTP GET request and read response (message) from the server
		HttpURLConnection con = null ;
		InputStream is = null;

		try {
			con = (HttpURLConnection) ( new URL(url+query)).openConnection();
			con.setRequestMethod("GET");
			con.setDoInput(true);
			con.connect();

			StringBuffer buffer = new StringBuffer();
			is = con.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while (  (line = br.readLine()) != null )
				buffer.append(line);
			String msg=buffer.toString();
			System.out.println("[Consumer status] A new message is received. Topic="+topic+" ID="+sessionID+" Message="+msg);
			return msg;
		}
		catch(Throwable t) {
			t.printStackTrace();
			System.out.println("[Consumer status] Failed to get a message from the server.");
		}
		finally {
			try { is.close(); } catch(Throwable t) {}
			try { con.disconnect(); } catch(Throwable t) {}
		}
		return null;		
	}

	public String getMessage(String topic) {//fire HTTP GET request and read response (message) from the server
		HttpURLConnection con = null ;
		InputStream is = null;
		String query="?topic="+topic+"&sessionID="+sessionID;
		try {
			con = (HttpURLConnection) ( new URL(url+query)).openConnection();
			con.setRequestMethod("GET");
			con.setDoInput(true);
			con.connect();

			StringBuffer buffer = new StringBuffer();
			is = con.getInputStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while (  (line = br.readLine()) != null )
				buffer.append(line);

			return buffer.toString();
		}
		catch(Throwable t) {
			t.printStackTrace();
			System.out.println("[Consumer status] Failed to get message from the server.");
		}
		finally {
			try { is.close(); } catch(Throwable t) {}
			try { con.disconnect(); } catch(Throwable t) {}
		}
		return null;		
	}

	private void buildQuery(){
		query="?topic="+topic+"&sessionID="+sessionID;
	}
	private void buildUrl(){
		url=baseURL+"/HyperQueue/Broker";
	}

	private void setSessionID(String id){this.sessionID=id;this.buildQuery();}

	public void setTopic(String topic){this.topic=topic;this.buildQuery();}
}
