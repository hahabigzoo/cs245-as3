package cs245.as3;

import cs245.as3.interfaces.LogManager;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.HashMap;

import cs245.as3.Record;

public class RecordManager {
    public final static int sizeOffset=12;
    public final static int head=16;
    public final static int mlength=128;
    private HashMap<Long, byte[]> recordsets;

    public int getLogSize(byte[] bytes){
        return (((bytes[sizeOffset] & 0xff) << 24) |
        ((bytes[sizeOffset + 1] & 0xff) << 16) |
        ((bytes[sizeOffset + 2] & 0xff) << 8) |
        (bytes[sizeOffset + 3] & 0xff));
    }
    
    public Record start(long txID){
        return new Record(txID, 0);
    }

    public long writeStart(long txID, LogManager lm){
        Record rd=new Record(txID, 0);
        byte[] bytes=Record.serialize(rd);
        return lm.appendLogRecord(bytes);
    } 

    public Record write(long txID, long key,  byte[] newValue){
        return new Record(txID, 1, key, newValue);
    }

    public long writeContext(long txID, LogManager lm, long key,  byte[] newValue){
        long tag=0;
        Record rd=new Record(txID, 1, key, newValue);
        byte[] bytes=Record.serialize(rd);
        int size=rd.size();
        ByteBuffer buff=ByteBuffer.wrap(bytes);
        for(int i=0;i<size;i=i+mlength){
            int l=Math.min(size-i,mlength);
            byte[] tmp=new byte[l];
            buff.get(tmp, i, l);
            tag=lm.appendLogRecord(tmp);
        }
        return tag;
    }

    public Record abort(long txID){
        return new Record(txID, 3);
    }

    public long writeAbort(long txID, LogManager lm){
        Record rd=new Record(txID, 3);
        byte[] bytes=Record.serialize(rd);
        return lm.appendLogRecord(bytes);
    }

    public Record commit(long txID){
        return new Record(txID, 2);
    }

    public long writeCommit(long txID, LogManager lm){
        Record rd=new Record(txID, 2);
        byte[] bytes=Record.serialize(rd);
        return lm.appendLogRecord(bytes);
    }

    public long writeRecord(LogManager lm ,Record rd ){
        long tag=0;
        byte[] bytes=Record.serialize(rd);
        int size=rd.size();
        ByteBuffer buff=ByteBuffer.wrap(bytes);
        for(int i=0;i<size;i=i+mlength){
            int l=Math.min(size-i,mlength);
            byte[] tmp=new byte[l];
            buff.get(tmp, i, l);
            tag=lm.appendLogRecord(tmp);
        }
        return tag;
    }

    public Record readRecord(LogManager lm,int offset){
        byte[] bytes=lm.readLogRecord(offset, head);
        int size=getLogSize(bytes);
        ByteBuffer buff=ByteBuffer.allocate(size);
        for(int i=0;i<size;i+=mlength){
            int l=Math.min(size-i,mlength);
            buff.put(lm.readLogRecord(offset+i, l));
        }
        byte[] recordbytes=buff.array();
        Record record=Record.deserialize(recordbytes);
        return record;
    }
    
}
