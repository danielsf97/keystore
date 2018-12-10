package keystore;


import org.apache.log4j.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

//https://github.com/luisacpcoutinho/DSP_Bank/blob/6b1a25fc7c8589437c4e5d2f0c48eb879e2101f6/DSP_bank/src/main/java/TwoPC/TwoPCLog.java
public class Log {

    private static PrintWriter out;

    private int size;
    private String name;

    public Log (String name){
        size = 0;
        this.name = name;
        setPrintWriter("keystore.log");
    }

    public static void setPrintWriter(String fileName)
    {
        try {
            out=new PrintWriter(new FileWriter(fileName),true);
            out.println("Id:Action:IdOperation");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    public void addCommitMark(int xid){
        size++;
        out.println(size + ":C:" + xid);
    }

    public void addPreparedMark(int xid){
        size++;
        out.println(size + ":P:" + xid);
    }

    public void addAbortMark(int xid){
        size++;
        out.println(size + ":A:" + xid);
    }



 /*   public void addResource (String resource, int xid) throws SQLException;
    public void addPreparedMark (int xid) throws SQLException;
    public void addCommitMark (int xid) throws SQLException;
    public void addAbortMark(int xid) throws SQLException;
    public void empty() throws SQLException;
    public String get(int index) throws SQLException;
    public int getXid (int index) throws SQLException;
    public ArrayList<String> getAll() throws SQLException;
*/

}

