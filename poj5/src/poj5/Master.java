package poj5;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.SecurityOperationsImpl;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.thrift.AuthInfo;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.util.Tool;

public class Master {

	public static void main(String[] args) {
		String instanceName = "myinstance";
		String zooServers = "zooserver-one,zooserver-two";
		Instance inst = new ZooKeeperInstance(instanceName, zooServers);

		try {
			String user = "root";
			String password ="acc";
			String instanceId = inst.getInstanceID();
			AuthInfo credentials = new AuthInfo(user, ByteBuffer.wrap(password.getBytes()), instanceId);
			Connector conn = inst.getConnector(credentials);
			conn.tableOperations().create("predict");
			SecurityOperationsImpl soi = new SecurityOperationsImpl(inst, credentials);
			
			//@To-Do get the info from the twitter api
			// rowID would be the name of the team and colFam would be the  type of info we are storing
			
			
			
			Text rowID = new Text("row1");
			Text colFam = new Text("myColFam");
			Text colQual = new Text("myColQual");
			ColumnVisibility colVis = new ColumnVisibility("public");
			long timestamp = System.currentTimeMillis();
			Value value = new Value("myValue".getBytes());
		    BatchWriter wr = conn.createBatchWriter("TABLEA", 10000000, 10000, 5);		    
			Mutation mutation = new Mutation(rowID);
			mutation.put(colFam, colQual, colVis, timestamp, value);
			

			wr.addMutation(mutation);
			wr.close();
		} catch (AccumuloException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AccumuloSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TableNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
