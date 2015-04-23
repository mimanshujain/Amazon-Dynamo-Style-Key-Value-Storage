package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Hashtable;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    //The Database reference needed for storage
    private SQLiteDatabase messengerDb;
    private static Uri mUri = null;
    //A reference for Db Helper Class
    private static GroupMessageDbHelper dbHelper;
    //Threading Attributes
    private static final BlockingQueue<Runnable> sPoolWorkQueue =
            new LinkedBlockingQueue<Runnable>(128);
    static final Executor myPool = new ThreadPoolExecutor(60,120,1, TimeUnit.SECONDS,sPoolWorkQueue);
    //Other Variables
    static String myPort = "";
    static String hashedPort = "";
    static boolean isRequester = true;
    private static BlockingQueue<String> queue = new SynchronousQueue<>();
    static ChordLinkList ring = null;
    static String originator = "";

    //Maps and Collections
    static Hashtable<String,String> remotePorts = new Hashtable<>();
    static Hashtable<String, Hashtable<String, TreeMap<Integer, String>>> replTable = new Hashtable<>();

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        messengerDb = dbHelper.getWritableDatabase();
        Log.v("Delete at "+myPort, "Key= "+selection);
        String select = "\"*\"";
        Log.v(selection.equals(select)+""," Select");
        int num = 0;

        if(selection.equals(DynamoResources.SELECTLOCAL)) {
            num = messengerDb.delete(DynamoResources.TABLE_NAME, null, null);
        }
        else if(selection.equals(DynamoResources.SELECTALL))
        {
            num = messengerDb.delete(DynamoResources.TABLE_NAME, null, null);
            String[] msg = new String[3];
            msg[0] = DynamoResources.DELETE;
            msg[1] = myPort;
            msg[2] = ring.head.next.port;
            new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
        }
        else
        {
            num = messengerDb.delete(DynamoResources.TABLE_NAME,"key = \'"+selection+"\'",null);
        }
        return num;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        try {
            String hashKey = genHash(values.get(DynamoResources.KEY_COL).toString());
            String coordinatorPort = lookUpCoordinator(hashKey);
            String key = values.get(DynamoResources.KEY_COL).toString();
            String value = values.getAsString(DynamoResources.VAL_COL);

            if(coordinatorPort.equals(myPort))
            {
                Log.d(DynamoResources.TAG,"Storing at my DB and sending to replicators");
                String msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + key + DynamoResources.separator +
                        value + DynamoResources.separator + myPort + DynamoResources.separator + hashKey;
                String[] msg = new String[2];
                msg[0] = DynamoResources.REPLICATION;
                msg[1] = msgToSend;
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
                myInsert(values);
            }

            else {
                Log.d(DynamoResources.TAG,"Sending Key to real Coordinator and replicators "+coordinatorPort);
                String msgToSend = key + DynamoResources.separator +
                        value + DynamoResources.separator + myPort + DynamoResources.separator + hashKey;
                String[] msg = new String[4];
                msg[0] = DynamoResources.COORDINATION;
                msg[1] = coordinatorPort;
                msg[2] = msgToSend;
                msg[3] = ring.getPreferenceList(coordinatorPort);
//                lookingSet.add(key);
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
            }
        }
        catch (NoSuchAlgorithmException ex)
        {
            ex.printStackTrace();
            Log.e(DynamoResources.TAG,ex.getMessage());
        }
		return null;
	}

    private synchronized void myInsert(ContentValues values) {
        messengerDb = dbHelper.getWritableDatabase();
        Cursor cur = messengerDb.query(DynamoResources.TABLE_NAME, new String[]{DynamoResources.VAL_COL},
                DynamoResources.KEY_COL + "='" + values.get(DynamoResources.KEY_COL) + "'", null, null, null, null);

        if (cur.getCount() > 0) {
            messengerDb.update(DynamoResources.TABLE_NAME, values, "value = '" + values.get(DynamoResources.VAL_COL) + "'", null);
        }
        else
        {
            long rowId = messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
            if (rowId > 0) {
//                myMessageMap.put(values.get(DynamoResources.KEY_COL).toString(),values.get(DynamoResources.VAL_COL).toString());
                Log.v(DynamoResources.TAG,"Insert at my Provider "+ values.toString());
//                return myUri;
            }
        }
    }

    private static String lookUpCoordinator(String hashKey)
    {
        Node head = ring.head;
        Node pre = head.previous;
        Node next = head.next;

        if(pre.hashPort.compareTo(hashedPort) > 0 && (hashKey.compareTo(pre.hashPort) > 0 || hashKey.compareTo(hashedPort) < 0))
        {
            Log.d(DynamoResources.TAG,"Looking Up: I will Keep key "+hashKey);
            return myPort;
        }
        else if(hashedPort.compareTo(hashKey) >= 0 && hashKey.compareTo(pre.hashPort) > 0)
        {
            Log.d(DynamoResources.TAG,"Looking Up: I will Keep key "+hashKey);
            return myPort;
        }
        else
        {
            while(true)
            {
                if(next.hashPort.compareTo(hashedPort) > 0 && (hashKey.compareTo(hashedPort) > 0 && next.hashPort.compareTo(hashKey) >= 0))
                    break;
                else if(next.hashPort.compareTo(hashedPort) < 0 && (hashKey.compareTo(hashedPort) > 0 && hashKey.compareTo(next.hashPort) < 0))
                    break;
                else
                    next = next.next;
            }
            return next.port;
        }

    }

	@Override
	public boolean onCreate() {
        Log.d(DynamoResources.TAG, "Inside Create");
        dbHelper = new GroupMessageDbHelper(getContext(), DynamoResources.DB_NAME, DynamoResources.DB_VERSION, DynamoResources.TABLE_NAME);

        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        messengerDb = dbHelper.getReadableDatabase();
        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
        qBuilder.setTables(DynamoResources.TABLE_NAME);
        Cursor resultCursor = null;
        try {
            if(selection.equals(DynamoResources.SELECTALL))
            {
                resultCursor = qBuilder.query(messengerDb, projection, null,
                        selectionArgs, null, null, sortOrder);
                Log.v(DynamoResources.TAG,"Select * local count: "+resultCursor.getCount());

                if(isRequester) {
                    originator = myPort;
                    MatrixCursor mx = getResults(selection, originator);
                    mx.moveToLast();
                    Log.v(DynamoResources.TAG,"Before Merging Count "+mx.getCount());
                    if (resultCursor.getCount() > 0) {
                        resultCursor.moveToFirst();
                        String cursorStr = getCursorValue(resultCursor);
                        String[] received = cursorStr.split(DynamoResources.valSeparator);
                        for (String m : received) {
                            String[] keyVal = m.split(" ");
                            mx.addRow(new String[]{keyVal[0], keyVal[1]});
                        }
                    }
                    mx.moveToFirst();
                    Log.v(DynamoResources.TAG, "After Merging Count " + mx.getCount());
                    return mx;
                }
                else
                {
                    if(originator.equals(ring.head.next.port))
                        return resultCursor;
                    else
                    {
                        MatrixCursor mx = getResults(selection,originator);
                        mx.moveToLast();
                        if (resultCursor.getCount() > 0) {
                            resultCursor.moveToFirst();
                            String cursorStr = getCursorValue(resultCursor);
                            String[] received = cursorStr.split(DynamoResources.valSeparator);
                            for (String m : received) {
                                String[] keyVal = m.split(" ");
                                mx.addRow(new String[]{keyVal[0], keyVal[1]});
                            }
                        }
                        mx.moveToFirst();
                        Log.v(DynamoResources.TAG, "After Merging Count " + mx.getCount());
                        return mx;
                    }
                }
            }
            else if(selection.equals(DynamoResources.SELECTLOCAL))
            {
                Log.v(DynamoResources.TAG,"Query "+selection);
                resultCursor = qBuilder.query(messengerDb, projection, null,
                        selectionArgs, null, null, sortOrder);
                Log.v(DynamoResources.TAG,"Count "+ resultCursor.getCount());
            }
            else
            {
                Log.v(DynamoResources.TAG,"Query a single key "+selection);
                resultCursor = qBuilder.query(messengerDb, projection, " key = \'" + selection + "\' ",
                        selectionArgs, null, null, sortOrder);
                if(resultCursor != null && resultCursor.getCount() <= 0)
                {
                    try {
                        return getResults(selection,myPort);
                    }
                    catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            Log.v(DynamoResources.TAG, "InterruptedException in Query");
        }
        catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
            Log.v(DynamoResources.TAG, "NoSuchAlgorithmException in Query");
        }


		return null;
	}

    private synchronized MatrixCursor getResults(String selection, String origin) throws InterruptedException, NoSuchAlgorithmException {
        Log.d(DynamoResources.TAG, "Inside GetResults Class for "+ selection);
        MatrixCursor mx = null;
        String coordinator = "";
        if(DynamoResources.SELECTALL.equals(selection)) {
            Log.d(DynamoResources.TAG, "Sending Query to next"+ ring.head.next.port);
            coordinator = ring.head.next.port;
        }
        else
        {
            Log.d(DynamoResources.TAG, "Sending Query to "+ coordinator);
            coordinator = lookUpCoordinator(genHash(selection));
        }

        String[] msg = new String[4];
        msg[0] = DynamoResources.QUERY;
        msg[1] = selection;
        msg[2] = coordinator;
        msg[3] = origin;
        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
        Log.d(DynamoResources.TAG, "Waiting...");
        String message = queue.take();
        Log.d(DynamoResources.TAG, "Gotcha Message " + message + " at " + myPort);
        String[] received = message.split(DynamoResources.valSeparator);
        for (String m : received) {
            Log.v(DynamoResources.TAG, "Merging my results");
            String[] keyVal = m.split(" ");
            mx.addRow(new String[]{keyVal[0], keyVal[1]});
        }
        Log.v(DynamoResources.TAG,"GetResults Count "+mx.getCount());
        mx = new MatrixCursor(new String[]{DynamoResources.KEY_COL, DynamoResources.VAL_COL}, 50);
        return mx;
    }
    
    private String getCursorValue(Cursor resultCursor) {
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
    
	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    Log.v(DynamoResources.TAG,"Socket Accepted");
                    InputStream read = socket.getInputStream();
                    String msgReceived = "";
                    InputStreamReader reader = new InputStreamReader(read);
                    BufferedReader buffer = new BufferedReader(reader);
                    msgReceived = buffer.readLine();
                    Log.v(DynamoResources.TAG,"Message Received :"+msgReceived);
                    String[] msgs = msgReceived.split(DynamoResources.separator);

                    if(msgs[0].equals(DynamoResources.JOINING))
                    {
                        Log.d(DynamoResources.TAG,"Joining starts for "+msgs[1]);

                        adjustNode(msgs[1]);
                        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.HEARTBEAT, msgs[1]});
                    }
                    else if(msgs[0].equals(DynamoResources.HEARTBEAT))
                    {
                        Log.d(DynamoResources.TAG, "Heartbeat of " + msgs[1]);
                        adjustNode(msgs[1]);
                    }
                    else if(msgs[0].equals(DynamoResources.COORDINATION))
                    {
                        Log.d(DynamoResources.TAG,"Coordination Message by "+msgs[3]);
                        ContentValues cv = new ContentValues();

                        cv.put(DynamoResources.KEY_COL, msgs[1]);
                        cv.put(DynamoResources.VAL_COL, msgs[2]);
                        myInsert(cv);

//                        String msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + msgs[1] + DynamoResources.separator +
//                                msgs[2] + DynamoResources.separator + myPort + DynamoResources.separator + msgs[4];
//                        String[] msg = new String[2];
//                        msg[0] = DynamoResources.REPLICATION;
//                        msg[1] = msgToSend;
//                        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
                    }
                    else if(msgs[0].equals(DynamoResources.REPLICATION))
                    {
                        Log.d(DynamoResources.TAG,"Replication Message by "+msgs[3]);
                        ContentValues cv = new ContentValues();
                        cv.put(DynamoResources.KEY_COL, msgs[1]);
                        cv.put(DynamoResources.VAL_COL, msgs[2]);
                        myInsert(cv);
                    }
                    else if(msgs [0].equals(DynamoResources.QUERY))
                    {
                        Log.d(DynamoResources.TAG,"Query Request "+myPort+" "+msgs[0]+" "+msgs[1]);
                        originator = msgs[3];
                        isRequester = false;
                        if(mUri == null)
                            mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                        publishProgress(new String[]{msgs[0],msgs[1],msgs[2]});
                    }
                    else if(msgs[0].equals(DynamoResources.QUERYREPLY))
                    {
                        if(msgs.length > 2) {
                            Log.d(DynamoResources.TAG, "Got a reply Mate " + msgs[2]);
                            queue.put(msgs[2]);
                        }
                        else
                        {
                            queue.put("");
                        }
                    }
                    else if(msgs[0].equals(DynamoResources.DELETE))
                    {
                        Log.d(DynamoResources.TAG,"Delete request "+msgs[0]+" "+msgs[1]);
                        if(!msgs[1].equals(myPort))
                        {
                            delete(mUri,DynamoResources.SELECTALL,null);
                            if(!ring.head.next.port.equals(msgs[1])) {
                                String[] msg = new String[3];
                                msg[0] = DynamoResources.DELETE;
                                msg[1] = msgs[1];
                                msg[2] = ring.head.next.port;
                                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
                            }
                        }
                    }
                }
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
                Log.e(DynamoResources.TAG,"Failure in ServerTask: "+ex.getMessage());
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
                Log.e(DynamoResources.TAG,"Failure in ServerTask: "+ex.getMessage());
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            super.onProgressUpdate(values);

            if (values[0].equals(DynamoResources.QUERY)) {
                Cursor resultCursor = query(mUri, null,
                        values[1], null, null);
                Log.d(DynamoResources.TAG,"Query Requested result count"+resultCursor.getCount()+"");
                String message = "";
                if(resultCursor.getCount() > 0)
                    message = getCursorValue(resultCursor);
                isRequester = true;
                Log.v(DynamoResources.TAG,"Now replying back with "+message);
                String[] msgToRequester = new String[4];
                msgToRequester[0] = DynamoResources.QUERYREPLY;
                msgToRequester[1] = values[1];
                msgToRequester[2] = message;
                msgToRequester[3] = values[2];
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msgToRequester);
            }
        }

    }

    public static class ClientTask extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... params) {

            String msgToSend = "";
            String portToSend = "";
            if(params[0].equals(DynamoResources.JOINING)) {
                msgToSend = params[0] + DynamoResources.separator + params[1];
                for(String port : remotePorts.keySet())
                {
                    if(!port.equals(myPort))
                        sendMessage(port,msgToSend);
                }
            }
            else if(params[0].equals(DynamoResources.HEARTBEAT)) {
                msgToSend = params[0] + DynamoResources.separator + myPort;
                portToSend = params[1];
                sendMessage(portToSend,msgToSend);
            }
            else if(params[0].equals(DynamoResources.COORDINATION))
            {
                portToSend = params[1];
                sendMessage(portToSend,DynamoResources.COORDINATION + DynamoResources.separator + params[2]);
                String[] replicators = params[3].split(DynamoResources.valSeparator);
                msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + params[2];
                sendMessage(replicators[0], msgToSend);
                sendMessage(replicators[1], msgToSend);
            }
            else if(params[0].equals(DynamoResources.REPLICATION))
            {
                msgToSend = params[1];
                sendMessage(ring.head.next.port, msgToSend);
                sendMessage(ring.head.next.next.port, msgToSend);
            }
            else if(params[0].equals(DynamoResources.QUERY))
            {
                msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + myPort + DynamoResources.separator + params[3];
                portToSend = params[2];
                sendMessage(portToSend,msgToSend);
            }
            else if(params[0].equals(DynamoResources.QUERYREPLY))
            {
                msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + params[2];
                portToSend = params[3];
                sendMessage(portToSend,msgToSend);
            }
            else if(params[0].equals(DynamoResources.DELETE))
            {
                msgToSend = params[0] + DynamoResources.separator + params[1];
                portToSend = params[2];
                sendMessage(portToSend,msgToSend);
            }
            return null;
        }
    }

    private static void sendMessage(String portToSend, String msgToSend)
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

    private static void adjustNode(String port) throws NoSuchAlgorithmException{
        String hash = genHash(remotePorts.get(port));
        Node node = ring.head;
        while(true) {

            if (node.next == node && node.previous == node) {
                ring.addNode(node, port, hash, DynamoResources.NEXT);

                Log.d(DynamoResources.TAG, "Current Ring: " + ring.printRing());
                break;
            }

            else {
                if ((hash.compareTo(hashedPort) > 0 && hash.compareTo(node.next.hashPort) < 0) ||
                        (hashedPort.compareTo(node.next.hashPort) > 0 && (hash.compareTo(hashedPort) > 0 || node.next.hashPort.compareTo(hash) > 0))) {
                    Log.d(DynamoResources.TAG, "Adding my next " + port);
                    ring.addNode(node, port, hash, DynamoResources.NEXT);

                    Log.d(DynamoResources.TAG, "Current Ring: " + ring.printRing());
                    break;
                }
                else if ((hash.compareTo(hashedPort) < 0 && hash.compareTo(node.previous.hashPort) > 0) ||
                        (node.previous.hashPort.compareTo(hashedPort) > 0 && (hash.compareTo(node.previous.hashPort) > 0 || hash.compareTo(hashedPort) < 0))) {
                    Log.d(DynamoResources.TAG, "Adding my previous " +port);
                    ring.addNode(node, port, hash, DynamoResources.PREVIOUS);

                    Log.d(DynamoResources.TAG, "Current Ring: " + ring.printRing());
                    break;
                }
                else {
                    Log.d(DynamoResources.TAG, "Sending to next " + port);
                    node = node.next;
                }
            }
        }
    }

    public static void startUp(String myPortNum)
    {
        Log.d(DynamoResources.TAG, "Inside Start Up");
        myPort = myPortNum;
        try {
            remotePorts.put("11108","5554");
            remotePorts.put("11120","5560");
            remotePorts.put("11116","5558");
            remotePorts.put("11124","5562");
            remotePorts.put("11112","5556");
            hashedPort = genHash(remotePorts.get(myPort));
            ring = new ChordLinkList(myPort,hashedPort);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public class GroupMessageDbHelper extends SQLiteOpenHelper
    {
        private String CREATE_TABLE = ""; //= "tbl_chatMessage";

        public GroupMessageDbHelper(Context context, String dbName, int dbVersion, String tableName)
        {
            super(context, dbName, null, dbVersion);
            if(!"".equals(tableName))
            {
                CREATE_TABLE = "CREATE TABLE " +
                        tableName +  " ( " + DynamoResources.KEY_COL + " TEXT PRIMARY KEY, " + DynamoResources.VAL_COL + " TEXT )";
            }
            //messengerDb.execSQL(CREATE_TABLE);

        }

        @Override
        public void onCreate(SQLiteDatabase db)
        {
            db.execSQL(CREATE_TABLE);
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        }
    }
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }
}

//Coordination and Replication message need not to have hashkey. Look into this. Remove if not needed till the final submission.
//Query functionality for a single key. Send request to coordinator directly.