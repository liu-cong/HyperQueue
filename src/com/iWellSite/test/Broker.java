package com.iWellSite.test;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class Broker
 */
@WebServlet("/Broker")
public class Broker extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final int timeout=1000;//the waiting time for the Consumer when the queue is empty
	private static volatile int sessionID=0;

	
	//A ConcurrentHashMap is used to map topic to its corresponding message queue
	//the queue is implemented by a Vector for three reasons: 1. Vector is synchronized compared to 
	//an ArrayList. 2. A Queue is not necessary because we never remove(poll) an element from 
	//the queue. we have to maintain every element in the queue for different Consumers. 3. Compared 
	//to a Queue, a Vector provides faster look-up for the element with location=offset
	ConcurrentHashMap<String, Vector<String>> topicMap=new ConcurrentHashMap<String,Vector<String>> ();
	
	//Another ConcurrentHashMap is used to map sessionID to offset. Since both the key and value
	//are integers, actually a simpler data structure such as array can do the work. However a 
	//ConcurrentHashMap is chosen for the sake of easier thread safety
	ConcurrentHashMap<Integer, Integer> idMap=new ConcurrentHashMap<Integer,Integer>();
	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public Broker() {
		super();
		
		sessionID=0;
		System.out.println("A new broker is created.");//for debug purposes
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		response.setContentType("text/plain");
		PrintWriter printWriter  = response.getWriter();
		if(request.getParameter("sessionID")==null)//the first connection of a new consumer. Assign a new ID to it
		{
			printWriter.println(Integer.toString(++sessionID));
			int offset=0;
			idMap.put(sessionID,offset);
			System.out.println("[Consumer] New session ID is assigned. ID="+sessionID+" Offset="+idMap.get(sessionID));
		}
		else{//if the Consumer sent in a message reques (that contains sessionID and topic)
			int id=Integer.parseInt(request.getParameter("sessionID"));
			String topic=request.getParameter("topic");
			String message=null;
			try {
				message = getMessage(id,topic);//get message from the queue for the given topic and sessionID
				System.out.println("[Consumer] New message is consumed. SessionID="+sessionID+" Offset="+idMap.get(sessionID)+" Topic="+topic+" Message="+message);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			printWriter.println(message);
		}	

	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		String topic = request.getParameter("topic");
		String message = request.getParameter("message");
		this.addMessage(message,topic);//add message to the corresponding message queue

		System.out.println("[Producer] New message added. Topic="+ topic+" Message="+message);
	}

	private void addMessage(String message, String topic){
		if(topicMap.containsKey(topic))//if adding message to an existing topic
		{
			Vector<String> topicQueue=(Vector<String>) topicMap.get(topic);//get the corresponding message queue
			topicQueue.add(message);//add message to the queue
		}
		else//if this topic doesn't exist, create a new queue for it
		{
			Vector<String> topicQueue=new Vector<String>();
			topicQueue.add(message);
			topicMap.put(topic, topicQueue);
		}

	}

	private String getMessage(int id, String topic) throws InterruptedException{
		int offset=0;
		
		if(idMap.containsKey(id))
		offset=(int) idMap.get(id);
		else return null;
		
		Vector<String> q=(Vector<String>) topicMap.get(topic);//get the message queue for the given topic
		
		String msg;
		if(q==null||offset>=q.size())
		{
			Thread.sleep(timeout);//wait for the producer to add new message for timeout ms.
			if(q!=null&&offset<q.size()){//after timeout ms, if the producer added a new message, then send it to the Consumer, otherwise send null
				msg=(String) q.get(offset);
				idMap.put(id, ++offset);//increment offset
			}
			else msg=null;
		} 
		else{
			msg=(String) q.get(offset);//get message from the message queue for the given offset
			idMap.put(id, ++offset);//increment offset
		}
		return msg;
	}

}
