package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.Cursor;
import android.net.Uri;
import android.util.Log;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/**
 * Created by sherlock on 4/20/15.
 */
public final class DynamoResources {

    //Message Types
    public final static String JOINING = "Joining";
    public final static String HEARTBEAT = "heartBeat";
    public final static String COORDINATION = "coor";
    public final static String REPLICATION = "repl";
    public final static String QUERY = "query";
    public final static String QUERYREPLY = "queryReply";
    public final static String DELETE = "delete";

    //Db Table and Column Name
    public static final String TABLE_NAME = "tblchatMessage";
    public static final String KEY_COL = "key";
    public static final String VAL_COL = "value";

    //Db Attributes
    public static final String DB_NAME = "messagesDb";
    public static final int DB_VERSION = 3;

    //Message Identifiers
    public static final String separator = "---";
    public static final String valSeparator = "##";
    public static final String SELECTALL = "\"*\"";
    public static final String SELECTLOCAL = "\"@\"";

    //Class Names
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    static final String RINGTAG = ChordLinkList.class.getSimpleName();

    //Node Location
    public static final String NEXT = "next";
    public static final String PREVIOUS = "previous";

    public static void sendMessage(String portToSend, String msgToSend)
    {
        try {
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(portToSend));
            OutputStream out = socket.getOutputStream();
            OutputStreamWriter writer = new OutputStreamWriter(out);
            writer.write(msgToSend);
            writer.flush();
            writer.close();
            out.close();
            socket.close();

        }
        catch (UnknownHostException e) {
            e.printStackTrace();
            Log.e(DynamoResources.TAG, "ClientTask UnknownHostException");
        }
        catch (IOException e) {
            e.printStackTrace();
            Log.e(DynamoResources.TAG, "ClientTask IOException for "+portToSend);
        }
        catch (Exception e){
            e.printStackTrace();
            Log.e(DynamoResources.TAG, "ClientTask Exception");
        }
    }

    public static String getCursorValue(Cursor resultCursor) {
        Log.v(DynamoResources.TAG,"Converting Cursor to String");
        resultCursor.moveToFirst();
        int valueIndex = resultCursor.getColumnIndex(DynamoResources.VAL_COL);
        int keyIndex = resultCursor.getColumnIndex(DynamoResources.KEY_COL);
        String result = "";
        boolean isLast = true;
        while(resultCursor.getCount() > 0 && isLast)
        {
            String newKey = resultCursor.getString(keyIndex);
            String newValue = resultCursor.getString(valueIndex);
            result = result+newKey+" "+newValue+DynamoResources.valSeparator;
            isLast = resultCursor.moveToNext();
        }
        Log.v(DynamoResources.TAG,"Final Building: "+result);
        return result;
    }

    public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}
