package cs245.as3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Map.Entry;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;
import cs245.as3.RecordManager;
/**
 * You will implement this class.
 *
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 *
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 *
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;

	private RecordManager rm;
	private LogManager lm;
	private StorageManager sm;
	private HashMap<Long, ArrayList<Record>> recordsets;
	private PriorityQueue<Long> que;
	private HashMap<Long,Integer> ttag; 


	public TransactionManager() {
		rm=new RecordManager();
		writesets = new HashMap<>();
		que=new PriorityQueue<>();
		ttag=new HashMap<Long,Integer>();
		recordsets=new HashMap<Long, ArrayList<Record>>();
		//see initAndRecover
		latestValues = null;
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm){
		this.lm=lm;
		this.sm=sm;
		latestValues = sm.readStoredTable();
		/**
		 * 读取所有日志文件
		 */
		ArrayList<Record> logRecords=new ArrayList<Record>();
		ArrayList<Integer> logtags=new ArrayList<Integer>();
		HashSet<Long> txCommit=new HashSet<Long>();
		for(int offset=lm.getLogTruncationOffset();offset<lm.getLogEndOffset();){
			Record rd=rm.readRecord(lm, offset);
			logRecords.add(rd);
			if(rd.getType()==2){
				//记录提交日志
				txCommit.add(rd.getTxID());
			}
			offset+=rd.size();
			logtags.add(offset);
		}
		/**
		 * 持久化：对已经提交的事务的日志重做,并持久化写入
		 */
		
		Iterator<Integer> it=logtags.iterator(); 
		for(Record rd:logRecords){
			if(txCommit.contains(rd.getTxID()) && rd.getType()==1){
				long tag = it.next();
				latestValues.put(rd.getKey(), new TaggedValue(tag, rd.getData()));
				que.add(tag);
				sm.queueWrite(rd.getKey(), tag, rd.getData());
			}
		}
		
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
		recordsets.put(txID, new ArrayList<Record>());
		//recordsets.get(txID).add(rm.start(txID));
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key. 
	 */
	public void write(long txID, long key, byte[] value) {
		//rm.writeContext(txID, lm, key, value);
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		recordsets.get(txID).add(rm.write(txID,key,value));
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset); 
		}
		writeset.add(new WritesetEntry(key, value));
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID){
		recordsets.get(txID).add(rm.commit(txID));
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		HashMap<Long,Long> keyTag=new HashMap<Long,Long>();
		for(Record rd: recordsets.get(txID)){
			if(rd.getType()==1){
				keyTag.put(rd.getKey(),rm.writeRecord(lm,rd));
			}else{
				rm.writeRecord(lm,rd);
			}
			
		}
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				long tag=keyTag.get(x.key);
				latestValues.put(x.key, new TaggedValue(tag, x.value));
				que.add(tag);
				sm.queueWrite(x.key, tag, x.value);
			}
			writesets.remove(txID);
		}
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		rm.abort(txID);
		recordsets.remove(txID);
		writesets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		if(persisted_tag==que.peek()){
			lm.setLogTruncationOffset((int)persisted_tag);
		}
		que.remove(persisted_tag);
	}
}
