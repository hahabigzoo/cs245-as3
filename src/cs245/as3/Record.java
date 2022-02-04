package cs245.as3;


import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.*;  

public class Record {
    /**
     * implement the redu log record
     * 日志的序列化与反序列化
     */
    
    private long txID; //8 bytes
    private long key;  //8 bytes
    /**
     * typr
     * 0:start
     * 1:write
     * 2:finish
     * 3:abort
     */
    private int type; //4 bytes
    private int size;
    private byte[] data;

    public Record(long txID,int type){
		this.txID=txID;
		this.type=type;
	}
	
	public Record(long txID,int type, long key, byte[] data){
		this.txID=txID;
		this.type=type;
		this.key=key;
		this.data=data;
        this.size();
	}

    public long getTxID(){
        return txID;
    }

    public int getType(){
        return type;
    }

    public long getKey(){
        return key;
    }
    public byte[] getData(){
        return data;
    }

	public int size(){
		size=16;
		if(type==1){
			size=size+8+data.length;
		}
		return size;
	}
    /**
     * 序列化
     */

    public static byte[] serialize(Record record) {
        try{
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream oos = new DataOutputStream(bos);
            oos.writeLong(record.txID);
            oos.writeInt(record.type);
            oos.writeInt(record.size());
            if(record.type==1){
                oos.writeLong(record.key);
                for(int i=0;i<record.data.length;i++){
                    oos.writeByte(record.data[i]);
                }
            }
            oos.flush();
            byte[] records = bos.toByteArray();
            oos.close();
            bos.close();
            return records;
        }catch(IOException e){
            return null;
        }
    }

    /**
     * 反序列化
     */
    public static Record deserialize(byte[] recordbytes) {
        ByteBuffer buff = ByteBuffer.wrap(recordbytes);
        long id=buff.getLong();
        Record record=new Record(id,buff.getInt());
		record.size=buff.getInt();
        if(record.type==1){
            record.key=buff.getLong();
            int size=record.size-24;
            record.data=new byte[size];
            for(int i=0;i<size;i++){
                record.data[i]=buff.get();
            }
        }
        return record;
    }
    @Override
    public String toString(){
		String str=""+this.txID+" "+this.type+" "+this.size()+" ";
		if(this.type==1){
			str+=this.key+" ";
			for(int i=0;i<data.length;i++){
				str+=data[i];
			}
		}
		return str;
	}
}
