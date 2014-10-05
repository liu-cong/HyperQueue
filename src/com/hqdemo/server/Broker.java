package com.hqdemo.server;

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
	ConcurrentHashMap<String, Vector<String>> topic_queue_map=new ConcurrentHashMap<String,Vector<String>> ();

	//Another ConcurrentHashMap is used to map sessionID to a topic-offset map. a topic-offset map is created
	//for each id-topic pair. Therefore, to find a message corresponding to the sessionID and topic from
	//a consumer, four steps are required: 1. map sessionID to a topic-offset map, 2. map topic to an offset
	//3. map topic to a message queue and 4. get the message, message=queue[offset]
	ConcurrentHashMap<Integer, ConcurrentHashMap<String, Integer>> id_topic_offset_map=new ConcurrentHashMap<Integer,ConcurrentHashMap<String,Integer>>();

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
			id_topic_offset_map.put(sessionID,new ConcurrentHashMap<String,Integer>());
			System.out.println("[Consumer] New session ID is assigned. ID="+sessionID+" Offset="+id_topic_offset_map.get(sessionID));
		}
		else{//if the Consumer sent in a message reques (that contains sessionID and topic)
			int id=Integer.parseInt(request.getParameter("sessionID"));
			String topic=request.getParameter("topic");
			String message=null;
			try {
				message = getMessage(id,topic);//get message from the queue for the given topic and sessionID
				if(message!=null)
					System.out.println("[Consumer] New message is consumed. SessionID="+id+" Offset="+id_topic_offset_map.get(id).get(topic)+" Topic="+topic+" Message="+message);
				else
					System.out.println("[Consumer] Invalid request, no message is consumed.");
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
		if(topic!=null&&message!=null)
		{this.addMessage(message,topic);//add message to the corresponding message queue
		System.out.println("[Producer] New message added. Topic="+ topic+" Message="+message);}
		else
			System.out.println("[Producer] Null topic or message");
	}

	private void addMessage(String message, String topic){

		if(topic_queue_map.containsKey(topic))//if adding message to an existing topic
		{
			Vector<String> topicQueue=(Vector<String>) topic_queue_map.get(topic);//get the corresponding message queue
			topicQueue.add(message);//add message to the queue
		}
		else//if this topic doesn't exist, create a new queue for it
		{
			Vector<String> topicQueue=new Vector<String>();
			topicQueue.add(message);
			topic_queue_map.put(topic, topicQueue);
		}

	}

	private String getMessage(int id, String topic) throws InterruptedException{
		int offset=0;
		ConcurrentHashMap<String, Integer> map=null;
		if(id_topic_offset_map.containsKey(id)){
			map= id_topic_offset_map.get(id);
			if(map.containsKey(topic)) {offset=map.get(topic);map.put(topic, offset+1);}
			else map.put(topic, 1);
		}

		else return null;

		Vector<String> q=(Vector<String>) topic_queue_map.get(topic);//get the message queue for the given topic

		String msg;
		if(q==null||offset>=q.size())
		{
			Thread.sleep(timeout);//wait for the producer to add new message for timeout ms.
			if(q!=null&&offset<q.size()){//after timeout ms, if the producer added a new message, then send it to the Consumer, otherwise send null
				msg=(String) q.get(offset);
				id_topic_offset_map.put(id, map);//increment offset
			}
			else msg=null;
		} 
		else{
			msg=(String) q.get(offset);//get message from the message queue for the given offset
			id_topic_offset_map.put(id, map);//increment offset
		}
		return msg;
	}

}
