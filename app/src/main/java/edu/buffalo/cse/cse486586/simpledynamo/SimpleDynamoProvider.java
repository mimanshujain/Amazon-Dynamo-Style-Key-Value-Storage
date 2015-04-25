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
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
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
import android.hardware.camera2.DngCreator;
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
    private static BlockingQueue<String> insertBlock = new SynchronousQueue<>();
    private static BlockingQueue<String> queryBlock = new SynchronousQueue<>();
    static ChordLinkList ring = null;
    static String originator = "";

    //Maps and Collections
    static Hashtable<String,String> remotePorts = new Hashtable<>();
//    static Hashtable<String, Hashtable<String, TreeMap<Integer, String>>> replTable = new Hashtable<>();
    static Set myKeySet = Collections.synchronizedSet(new HashSet<String>());
    static Hashtable<String, Hashtable<String, String>> failedCoorMap = new Hashtable<>();
    static Hashtable<String, Integer> failedVersions = new Hashtable<>();

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
            msg[2] = ring.getLifeStatus(ring.head.next.port)?ring.head.next.port:ring.head.next.next.port;
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
            String key = values.get(DynamoResources.KEY_COL).toString();
            String value = values.getAsString(DynamoResources.VAL_COL);
            Log.d(DynamoResources.TAG,"Insertion Starts for key "+key);
            String hashKey = DynamoResources.genHash(values.get(DynamoResources.KEY_COL).toString());
            //Versioning
            int currentVersion = 0;
            if(values.containsKey(DynamoResources.VERSION))
                currentVersion = values.getAsInteger(DynamoResources.VERSION);
            else
            {
                currentVersion = 1;
                values.put(DynamoResources.VERSION, currentVersion);
            }

            String coordinatorPort = lookUpCoordinator(key, hashKey);
            if(coordinatorPort.equals(myPort))
            {
                Log.d(DynamoResources.TAG,"Storing at my DB and sending to replicators");
                messengerDb = dbHelper.getWritableDatabase();
                Cursor cur = messengerDb.query(DynamoResources.TABLE_NAME, new String[]{DynamoResources.VAL_COL},
                        DynamoResources.KEY_COL + "='" + values.get(DynamoResources.KEY_COL) + "'", null, null, null, null);

                if (cur.getCount() > 0) {
                    int oldVersion = cur.getInt(cur.getColumnIndex(DynamoResources.VERSION));
                    currentVersion = oldVersion + 1;
                    values.put(DynamoResources.VERSION,currentVersion);
//                messengerDb.update(DynamoResources.TABLE_NAME, values, "value = '" + values.get(DynamoResources.VAL_COL) + "'", null);
                    messengerDb.update(DynamoResources.TABLE_NAME, values, null, null);
                }
                else
                {
                    currentVersion = 1;
                    values.put(DynamoResources.VERSION, currentVersion);
//                    myInsert(values);
                    messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
                    myKeySet.add(key);
                }

                //See if record is present
                //if no - then add version column and call myinsert and also send replication message
                //if yes - then find the older version number, +1 it and update it here only and also send the updated version to replicators

                String msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + key + DynamoResources.separator +
                        value + DynamoResources.separator + myPort + DynamoResources.separator + hashKey +
                        DynamoResources.separator + currentVersion;
                String[] msg = new String[2];
                msg[0] = DynamoResources.REPLICATION;
                msg[1] = msgToSend;
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
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

    private synchronized int myInsert(ContentValues values, String type) throws NoSuchAlgorithmException{

        messengerDb = dbHelper.getWritableDatabase();
        Cursor cur = messengerDb.query(DynamoResources.TABLE_NAME, new String[]{DynamoResources.VAL_COL},
                DynamoResources.KEY_COL + "='" + values.get(DynamoResources.KEY_COL) + "'", null, null, null, null);

        if(type.equals(DynamoResources.REPLICATION)) {
            Log.d(DynamoResources.TAG, "Inserting into my DB key " + values.get(DynamoResources.KEY_COL));

            if (cur.getCount() > 0) {
                Log.d(DynamoResources.TAG, "Already present, Updating with new version");
                int oldVersion = cur.getInt(cur.getColumnIndex(DynamoResources.VERSION));
                int newVersion = values.getAsInteger(DynamoResources.VERSION);
                if (newVersion > oldVersion) {
                    values.put(DynamoResources.VERSION, newVersion);
//                messengerDb.update(DynamoResources.TABLE_NAME, values, "value = '" + values.get(DynamoResources.VAL_COL) + "'", null);
                    messengerDb.update(DynamoResources.TABLE_NAME, values, null, null);
                    return Math.max(newVersion,oldVersion);
                }
            } else {
                Log.d(DynamoResources.TAG, "Inserting new row");
                long rowId = messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
            }
        }//if(type.equals(DynamoResources.FAILED))
        else {
            Log.d(DynamoResources.TAG, "Inserting into my DB for failed or coordination key " + values.get(DynamoResources.KEY_COL));
            if (cur.getCount() > 0) {
                int oldVersion = cur.getInt(cur.getColumnIndex(DynamoResources.VERSION));
                values.put(DynamoResources.VERSION, oldVersion+1);
//                messengerDb.update(DynamoResources.TABLE_NAME, values, "value = '" + values.get(DynamoResources.VAL_COL) + "'", null);
                messengerDb.update(DynamoResources.TABLE_NAME, values, null, null);
                return oldVersion + 1;

            }
            else {
                values.put(DynamoResources.VERSION, 1);
                Log.d(DynamoResources.TAG, "Inserting new row");
                long rowId = messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
                return 1;
            }
        }

        return 0;
    }

	@Override
	public boolean onCreate() {
        Log.d(DynamoResources.TAG, "Inside Create");
        dbHelper = new GroupMessageDbHelper(getContext(), DynamoResources.DB_NAME, DynamoResources.DB_VERSION, DynamoResources.TABLE_NAME);

        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {

        projection = new String[2];
        projection[0] = DynamoResources.KEY_COL;
        projection[1] = DynamoResources.VAL_COL;
        Log.d(DynamoResources.TAG, "Inside Query");
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
                        String cursorStr = DynamoResources.getCursorValue(resultCursor);
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
                            String cursorStr = DynamoResources.getCursorValue(resultCursor);
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
		return resultCursor;
	}

    private synchronized MatrixCursor getResults(String selection, String origin) throws InterruptedException, NoSuchAlgorithmException {
        Log.d(DynamoResources.TAG, "Inside GetResults Class for "+ selection);
        MatrixCursor mx = null;
        String coordinator = "";
        if(DynamoResources.SELECTALL.equals(selection)) {
            Log.d(DynamoResources.TAG, "Sending Query to next"+ ring.head.next.port);
            if(ring.getLifeStatus(ring.head.next.port))
                coordinator = ring.head.next.port;
            else
                coordinator = ring.head.next.next.port;
        }
        else
        {
            coordinator = lookUpCoordinator(selection, DynamoResources.genHash(selection));
            Log.d(DynamoResources.TAG, "Sending Query to "+ coordinator);
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
        mx = new MatrixCursor(new String[]{DynamoResources.KEY_COL, DynamoResources.VAL_COL}, 50);
        for (String m : received) {
            Log.v(DynamoResources.TAG, "Merging my results");
            String[] keyVal = m.split(" ");
            if(!myKeySet.contains(keyVal[0]))
                mx.addRow(new String[]{keyVal[0], keyVal[1]});
        }
        Log.v(DynamoResources.TAG,"GetResults Count "+mx.getCount());
        return mx;
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
//                    Log.v(DynamoResources.TAG,"Socket Accepted");
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
                        if(!ring.existsInChain(msgs[1]))
                            adjustNode(msgs[1]);
                        else {
                            ring.setLifeStatus(msgs[1], true);
                            String myCoord = ring.getPreferenceList(msgs[1]);
                            if(myCoord.split(DynamoResources.valSeparator)[0].equals(myPort))
                            {
                                String message = recovery(msgs[1]);
                                Log.d(DynamoResources.TAG, "Sending Recovery Message to "+msgs[1]);
                                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.RECOVERY, message, msgs[1]});
                            }
                        }
                        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.HEARTBEAT, msgs[1]});

                    }
                    else if(msgs[0].equals(DynamoResources.HEARTBEAT))
                    {
                        Log.d(DynamoResources.TAG, "Heartbeat of " + msgs[1]);
                        adjustNode(msgs[1]);
                    }
                    else if(msgs[0].equals(DynamoResources.COORDINATION))
                    {
                        Log.d(DynamoResources.TAG,"Coordination Message by "+msgs[3]+" for key "+msgs[1]);
                        ContentValues cv = new ContentValues();

                        cv.put(DynamoResources.KEY_COL, msgs[1]);
                        cv.put(DynamoResources.VAL_COL, msgs[2]);
                        int version = myInsert(cv,"");

                        String msgToSend = DynamoResources.ALIVE + DynamoResources.separator + version;
//                                + DynamoResources.separator +
//                                msgs[2] + DynamoResources.separator + myPort + DynamoResources.separator + msgs[4];
                        String[] msg = new String[3];
                        msg[0] = DynamoResources.ALIVE;
                        msg[1] = msgToSend;
                        msg[2] = msgs[3];
                        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);

                        myKeySet.add(msgs[1]);
                    }
                    else if(msgs[0].equals(DynamoResources.REPLICATION))
                    {
                        if(!msgs[5].equals(DynamoResources.FAILED)) {
                            Log.d(DynamoResources.TAG, "Replication Message by " + msgs[3] + " for key " + msgs[1] + " with version " + msgs[5]);
                            ContentValues cv = new ContentValues();
                            cv.put(DynamoResources.KEY_COL, msgs[1]);
                            cv.put(DynamoResources.VAL_COL, msgs[2]);
                            cv.put(DynamoResources.VERSION, Integer.parseInt(msgs[5]));
                            myInsert(cv,DynamoResources.REPLICATION);
                        }
                        else {
                            Log.d(DynamoResources.TAG, "Replication Message by " + msgs[3] + " for key " + msgs[1] + " with version " + msgs[5]);
                            ContentValues cv = new ContentValues();
                            cv.put(DynamoResources.KEY_COL, msgs[1]);
                            cv.put(DynamoResources.VAL_COL, msgs[2]);
                            cv.put(DynamoResources.COOR, msgs[6]);

                            int version = myInsert(cv,DynamoResources.FAILED);
                            if(msgs.length == 7)
                            {
                                if(failedCoorMap.contains(msgs[6]))
                                {
                                    Hashtable<String, String> dummy = failedCoorMap.get(msgs[6]);
                                    dummy.put(msgs[1], msgs[2]);
                                    failedCoorMap.put(msgs[6], dummy);
                                    failedVersions.put(msgs[1], version);
                                }
                                else
                                {
                                    Hashtable<String, String> dummy = new Hashtable<>();
                                    dummy.put(msgs[1], msgs[2]);
                                    failedCoorMap.put(msgs[6], dummy);
                                    failedVersions.put(msgs[1], version);
                                }
                            }
                        }
                    }
                    else if(msgs [0].equals(DynamoResources.QUERY))
                    {
                        Log.d(DynamoResources.TAG,"Query Request "+myPort+" "+msgs[0]+" "+msgs[1]);
                        originator = msgs[3];
                        isRequester = false;
                        if(mUri == null)
                            mUri = DynamoResources.buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
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
                    else if(msgs[0].equals(DynamoResources.ALIVE))
                    {
                        Log.d(DynamoResources.TAG, "Alive message");
                        insertBlock.add(msgs[1]);
                    }
                    else if(msgs[0].equals(DynamoResources.RECOVERY))
                    {
                        Log.d(DynamoResources.TAG, "REcovery message");
                        publishProgress(new String[]{msgs[0], msgs[1]});
                    }
                }
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
                Log.e(DynamoResources.TAG,"IOException in ServerTask: "+ex.getMessage());
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
                Log.e(DynamoResources.TAG,"Exception in ServerTask: "+ex.getMessage());
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
                    message = DynamoResources.getCursorValue(resultCursor);
                isRequester = true;
                Log.v(DynamoResources.TAG,"Now replying back with "+message);
                String[] msgToRequester = new String[4];
                msgToRequester[0] = DynamoResources.QUERYREPLY;
                msgToRequester[1] = values[1];
                msgToRequester[2] = message;
                msgToRequester[3] = values[2];
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msgToRequester);
            }
            else if(values[0].equals(DynamoResources.RECOVERY))
            {
                String message = values[1];
                String[] keyVal = message.split(DynamoResources.valSeparator);
                for(String kv : keyVal)
                {
                    String[] m = kv.split(" ");
                    ContentValues cv = new ContentValues();
                    cv.put(DynamoResources.KEY_COL, m[0]);
                    cv.put(DynamoResources.VAL_COL, m[1]);
                    cv.put(DynamoResources.VERSION, Integer.parseInt(m[2]));
                    try {
                        myInsert(cv,DynamoResources.REPLICATION);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
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
                        DynamoResources.sendMessage(port,msgToSend);
                }
            }
            else if(params[0].equals(DynamoResources.HEARTBEAT)) {
                msgToSend = params[0] + DynamoResources.separator + myPort;
                portToSend = params[1];
                DynamoResources.sendMessage(portToSend,msgToSend);
            }
            else if(params[0].equals(DynamoResources.COORDINATION))
            {
                portToSend = params[1];
                DynamoResources.sendMessage(portToSend,DynamoResources.COORDINATION + DynamoResources.separator + params[2]);
                String[] replicators = params[3].split(DynamoResources.valSeparator);
                if(ring.getLifeStatus(portToSend)) {
                    try {
                        String version = insertBlock.poll(2000, TimeUnit.MILLISECONDS);
                        Log.d(DynamoResources.TAG, "Coordinator replied with version " + version);
                        msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + params[2] +
                                DynamoResources.separator + version;

                    } catch (InterruptedException e) {
                        Log.d(DynamoResources.TAG, "Time out");
                        ring.setLifeStatus(portToSend, false);
                        msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + params[2] +
                                DynamoResources.separator + DynamoResources.FAILED
                                + DynamoResources.separator + portToSend;
                        e.printStackTrace();
                    }
                }
                else {
                    msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + params[2] +
                            DynamoResources.separator + DynamoResources.FAILED +DynamoResources.separator + portToSend;;
                }

//                msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + params[2];
                DynamoResources.sendMessage(replicators[0], msgToSend);
                DynamoResources.sendMessage(replicators[1], msgToSend);
            }
            else if(params[0].equals(DynamoResources.REPLICATION))
            {
                msgToSend = params[1];
                DynamoResources.sendMessage(ring.head.next.port, msgToSend);
                DynamoResources.sendMessage(ring.head.next.next.port, msgToSend+DynamoResources.separator+"2");
            }
            else if(params[0].equals(DynamoResources.QUERY))
            {
                msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + myPort + DynamoResources.separator + params[3];
                portToSend = params[2];
                DynamoResources.sendMessage(portToSend,msgToSend);
            }
            else if(params[0].equals(DynamoResources.QUERYREPLY))
            {
                msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + params[2];
                portToSend = params[3];
                DynamoResources.sendMessage(portToSend,msgToSend);
            }
            else if(params[0].equals(DynamoResources.DELETE))
            {
                msgToSend = params[0] + DynamoResources.separator + params[1];
                portToSend = params[2];
                DynamoResources.sendMessage(portToSend,msgToSend);
            }
            else if(params[0].equals(DynamoResources.ALIVE))
            {
                msgToSend = params[1];
                portToSend = params[2];
                DynamoResources.sendMessage(portToSend,msgToSend);
            }
            else if(params[0].equals(DynamoResources.RECOVERY))
            {
                msgToSend = DynamoResources.RECOVERY + DynamoResources.separator + params[1];
                portToSend = params[2];
                DynamoResources.sendMessage(portToSend,msgToSend);
            }
            return null;
        }
    }

    private static void adjustNode(String port) throws NoSuchAlgorithmException{
        String hash = DynamoResources.genHash(remotePorts.get(port));
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
            hashedPort = DynamoResources.genHash(remotePorts.get(myPort));
            ring = new ChordLinkList(myPort,hashedPort);

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private static String lookUpCoordinator(String key, String hashKey)
    {
        synchronized (ring) {
            Node current = ring.head;
            Node pre = current.previous;
//            Node next = current.next;
            String coordinator = "";
            Log.d(DynamoResources.TAG, "Looking up where to store the "+key);

            while(true)
            {
                if (pre.hashPort.compareTo(current.hashPort) > 0 && (hashKey.compareTo(pre.hashPort) > 0 || hashKey.compareTo(current.hashPort) < 0))
                {
                    coordinator = current.port;
                    break;
                }
                else if (current.hashPort.compareTo(hashKey) >= 0 && hashKey.compareTo(pre.hashPort) > 0)
                {
                    coordinator = current.port;
                    break;
                }
                else
                {
                    pre = current;
                    current = current.next;
                }

                if(current == ring.head)
                    break;
            }

            return coordinator;
        }
    }

    private String recovery(String port)
    {
        String result = "";
        if(failedCoorMap.contains(port))
        {
            Hashtable<String, String> dummy = failedCoorMap.get(port);
            for(Map.Entry<String, String> entry : dummy.entrySet())
            {
                result = result + entry.getKey() + " " + entry.getValue() + failedVersions.get(entry.getKey()) + DynamoResources.valSeparator;
            }
            failedCoorMap.put(port, new Hashtable<String, String>());
        }
        return result;
    }

}

//Coordination and Replication message need not to have hashkey. Look into this. Remove if not needed till the final submission.
//Query functionality for a single key. Send request to coordinator directly.
//* queries should return the unique values.