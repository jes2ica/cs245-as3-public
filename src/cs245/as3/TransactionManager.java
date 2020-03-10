package cs245.as3;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.*;

import cs245.as3.driver.LogManagerImpl;
import cs245.as3.driver.LogManagerTests;
import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

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
	class Record {
		long txID;
		long key;
		byte[] value;

		Record(long txID, long key, byte[] value) {
			this.txID = txID;
			this.key = key;
			this.value = value;
		}

		public byte[] serialize() {
			ByteBuffer ret = ByteBuffer.allocate(2 * Long.BYTES + value.length);
			ret.putLong(txID);
			ret.putLong(key);
			ret.put(value);
			return ret.array();
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	private LinkedHashMap<Long, ArrayList<Record>> txnLogRecords;
	private HashSet<Long> successfulTxns;
	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;
	private LogManager lm;
	private StorageManager sm;

	public TransactionManager() {
		writesets = new HashMap<>();
		successfulTxns = new HashSet<>();
		txnLogRecords = new LinkedHashMap<>();
		//see initAndRecover
		latestValues = null;
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		this.sm = sm;
		this.lm = lm;

		latestValues = sm.readStoredTable();
		redoLogs(lm, sm);
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		txnLogRecords.put(txID, new ArrayList<>());
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
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset == null) {
			writeset = new ArrayList<>();
			writesets.put(txID, writeset);
		}
		writeset.add(new WritesetEntry(key, value));
		txnLogRecords.get(txID).add(new Record(txID, key, value));
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset != null) {
			for (WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				long tag = 0;
				latestValues.put(x.key, new TaggedValue(tag, x.value));
			}
			writesets.remove(txID);

			appendRecord(lm, txID);
		}
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		txnLogRecords.remove(txID);
		writesets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
//		System.out.println("persisted: " + key + ":" + persisted_value);
	}

	private void appendRecord(LogManager lm, long txID) {
		String str = "COMMITED:" + txID;
		txnLogRecords.get(txID).add(new Record(txID, 0, str.getBytes()));

		List<Integer> offsets = new ArrayList<>();
		for (Record r : txnLogRecords.get(txID)) {
			byte[] r_byte = r.serialize();
			int r_len = r_byte.length;
			ByteBuffer ret = ByteBuffer.allocate(Long.BYTES + r_len);
			ret.putLong(r_len);
			ret.putLong(txID);
			ret.putLong(r.key);
			ret.put(r.value);
			offsets.add(lm.appendLogRecord(ret.array()));
		}
	}

	private void redoLogs(LogManager lm, StorageManager sm) {
		int offset = lm.getLogTruncationOffset();
		while (offset < lm.getLogEndOffset()) {
			byte[] length = lm.readLogRecord(offset, Long.BYTES);
			ByteBuffer bb = ByteBuffer.wrap(length);
			byte[] content = lm.readLogRecord(offset + Long.BYTES, (int) bb.getLong());
			Record record = deserialize(content);
			String recordStr = new String(record.value);
			if (!recordStr.startsWith("COMMITED")) {
				txnLogRecords.putIfAbsent(record.txID, new ArrayList<>());
				txnLogRecords.get(record.txID).add(record);
			} else {
				successfulTxns.add(record.txID);
			}
			offset += Long.BYTES + content.length;
		}
		for (long txID : txnLogRecords.keySet()) {
			if (!successfulTxns.contains(txID)) {
				continue;
			}
			ArrayList<Record> records = txnLogRecords.get(txID);
			for (Record record : records) {
				latestValues.put(record.key, new TaggedValue(0, record.value));
				sm.queueWrite(record.key, 0, record.value);
			}
		}
	}

	private Record deserialize(byte[] b) {
		ByteBuffer bb = ByteBuffer.wrap(b);
		long txID = bb.getLong();
		long key = bb.getLong();
		byte[] value = new byte[b.length - Long.BYTES - Long.BYTES];
		bb.get(value);
		return new Record(txID, key, value);
	}
}
