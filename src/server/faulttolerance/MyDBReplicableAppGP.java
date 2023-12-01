package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.PreparedStatement;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Config;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.FileReader;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

	/**
	 * Set this value to as small a value with which you can get tests to still
	 * pass. The lower it is, the faster your implementation is. Grader* will
	 * use this value provided it is no greater than its MAX_SLEEP limit.
	 * Faster
	 * is not necessarily better, so don't sweat speed. Focus on safety.
	 */
	public static final int SLEEP = 1000;
	
	final private Session session;
    final private Cluster cluster;
    PreparedStatement insertStatement;

	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		// TODO: setup connection to the data store and keyspace
		
		// Connect to the cluster and keyspace
		
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect(args[0]);
		insertStatement = session.prepare("INSERT INTO grade (id, events) VALUES (?, ?)");
	}

	/**
	 * Refer documentation of {@link Replicable#execute(Request, boolean)} to
	 * understand what the boolean flag means.
	 * <p>
	 * You can assume that all requests will be of type {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
	 *
	 * @param request
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		// TODO: submit request to data store
		return this.execute(request);
	}

	/**
	 * Refer documentation of
	 * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
	 *
	 * @param request
	 * @return
	 */
	@Override
	public boolean execute(Request request) {
		// TODO: execute the request by sending it to the data store
		if (request instanceof RequestPacket) {
			String requestValue = ((RequestPacket) request).requestValue;
			try {
				ResultSet resultSet = session.execute(requestValue);
				// Check if the query was successful
			       if (resultSet.wasApplied()) {
			    		return true;
					
			        } else {
			        	System.err.println("Query execution failed ");
			        }
				
			} catch(NumberFormatException nfe) {
				nfe.printStackTrace();
			}
		}
		else{
			System.err.println("Unknown request type: " + request.getRequestType());
		}
		return true;
	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		// TODO:
		
        ResultSet resultSet = session.execute("SELECT * FROM grade");
        
        StringBuilder formattedData = new StringBuilder();

        // Write data to the string with proper delimiters
        for (Row row : resultSet) {
            int id = row.getInt("id");
            String events = row.getList("events", Integer.class).toString();

            // Format the values and append to the string
            String formattedRow = String.format("%d|%s;", id, events); // Using ';' as row delimiter and '|' as column delimiter
            formattedData.append(formattedRow);
        }
        return formattedData.toString();
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */
	@Override
	public boolean restore(String s, String dataString) {
	    try {
	    	
	        // Check if the string data is empty
	        if (dataString.isEmpty() || dataString.equals("{}")) {
	            return true;
	        } else {
	            
	            // Split the string into individual rows
	            String[] rows = dataString.split(";");
	            
	            // Process and insert each row into the table
	            
	            for (String row : rows) {
	                String[] parts = row.split("\\|");
	                if (parts.length < 2) {
	                    System.err.println("Invalid row format: " + row);
	                    continue;
	                }

	                int id = parts[0].isEmpty() ? 0 : Integer.parseInt(parts[0]);
	                String events = parts[1];

	                if (events.isEmpty()) {
	                    session.execute(insertStatement.bind(id, Collections.emptyList()));
	                } else {
	                    String[] eventsArray = events.substring(1, events.length() - 1).split(",");
	                    List<Integer> eventsList = Arrays.stream(eventsArray)
	                            .map(String::trim)
	                            .map(str -> str.isEmpty() ? 0 : Integer.parseInt(str))
	                            .collect(Collectors.toList());

	                    session.execute(insertStatement.bind(id, eventsList));
	                }
	            }
	            return true;
	        }
	    } catch (IllegalArgumentException e) {
	        e.printStackTrace();
	        System.err.println("Could not restore state");
	    }
	    return true;
	}

	

	/**
	 * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
	 * .RequestPacket will be used by Grader, so you don't need to implement
	 * this method.}
	 *
	 * @param s
	 * @return
	 * @throws RequestParseException
	 */
	@Override
	public Request getRequest(String s) throws RequestParseException {
		return null;
	}

	/**
	 * @return Return all integer packet types used by this application. For an
	 * example of how to define your own IntegerPacketType enum, refer {@link
	 * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
	 * need to be implemented because the assignment Grader will only use
	 * {@link
	 * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		return new HashSet<IntegerPacketType>();
	}
}
