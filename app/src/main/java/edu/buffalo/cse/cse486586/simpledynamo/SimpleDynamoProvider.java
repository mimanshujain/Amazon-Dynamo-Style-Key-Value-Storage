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
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.Formatter;
import java.util.Hashtable;
import java.util.Timer;
import java.util.TimerTask;
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
    static int count = 0;
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
    static Set myKeySet = Collections.synchronizedSet(new HashSet<String>());
    static Hashtable<String, Hashtable<String, String>> failedCoorMap = new Hashtable<>();
    static Hashtable<String, Integer> failedVersions = new Hashtable<>();
    static Hashtable<String, String> myKeysMap = new Hashtable<>();
    static Hashtable<String, String> keyValMap = new Hashtable<>();
    static Hashtable<String, Integer> queryCount = new Hashtable<>();
    static Hashtable<String, String> queryKeyVal = new Hashtable<>();
    static Hashtable<String, Integer> queryKeyVersion = new Hashtable<>();
    static Hashtable<String, Timer> timerMap = new Hashtable<>();

    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        messengerDb = dbHelper.getWritableDatabase();
        Log.v(DynamoResources.TAG,"Count "+count++ +" Delete at "+myPort +"Key= "+selection);
        String select = "\"*\"";
        Log.v(selection.equals(select)+""," Select");
        int num = 0;

        if(selection.equals(DynamoResources.SELECTLOCAL)) {
            num = messengerDb.delete(DynamoResources.TABLE_NAME, null, null);
            myKeysMap = new Hashtable<>();
        }
        else if(selection.equals(DynamoResources.SELECTALL))
        {
            num = messengerDb.delete(DynamoResources.TABLE_NAME, null, null);
            myKeysMap = new Hashtable<>();
            String[] msg = new String[3];
            msg[0] = DynamoResources.DELETE;
            msg[1] = myPort;
            msg[2] = ring.getLifeStatus(ring.head.next.port)?ring.head.next.port:ring.head.next.next.port;
            new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
        }
        else
        {
            num = messengerDb.delete(DynamoResources.TABLE_NAME,"key = \'"+selection+"\'",null);
            myKeysMap.remove(selection);
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
            Log.d(DynamoResources.TAG,"Count "+count++ +" Insertion Starts for key "+key +" with value "+value);
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
                Log.d(DynamoResources.TAG,"Count "+count++ +" Storing at my DB and sending to replicators for key "+key);
                messengerDb = dbHelper.getWritableDatabase();
                Cursor cur = messengerDb.query(DynamoResources.TABLE_NAME, null,
                        DynamoResources.KEY_COL + "='" + values.get(DynamoResources.KEY_COL) + "'", null, null, null, null);

                if (cur.getCount() > 0) {
                    cur.moveToFirst();
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Count2 "+cur.getCount()+" Column Count "+cur.getColumnCount()+" Index: "+cur.getColumnName(2));
                    int oldVersion = cur.getInt(2);
                    currentVersion = oldVersion + 1;
                    if(values.size() == 3)
                        values.remove(DynamoResources.VERSION);
                    values.put(DynamoResources.VERSION,currentVersion);
//                messengerDb.update(DynamoResources.TABLE_NAME, values, "value = '" + values.get(DynamoResources.VAL_COL) + "'", null);
                    messengerDb.update(DynamoResources.TABLE_NAME, values, DynamoResources.KEY_COL+" = '" + values.get(DynamoResources.KEY_COL) + "'", null);
                }
                else
                {
                    currentVersion = 1;
                    if(values.size() == 3)
                        values.remove(DynamoResources.VERSION);
                    values.put(DynamoResources.VERSION, currentVersion);
//                    myInsert(values);
                    messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
//                    myKeySet.add(key);

                }
                myKeysMap.put(key,value);
                failedVersions.put(key,currentVersion);
                //See if record is present
                //if no - then add version column and call myinsert and also send replication message
                //if yes - then find the older version number, +1 it and update it here only and also send the updated version to replicators

                String msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + key + DynamoResources.separator +
                        value + DynamoResources.separator + myPort + DynamoResources.separator + hashKey +
                        DynamoResources.separator + currentVersion + DynamoResources.separator + myPort;
                String[] msg = new String[2];
                msg[0] = DynamoResources.REPLICATION;
                msg[1] = msgToSend;
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
            }

            else {
                Log.d(DynamoResources.TAG,"Count "+count++ +" Sending Key "+key+" to real Coordinator and replicators "+coordinatorPort);
                String msgToSend = key + DynamoResources.separator +
                        value + DynamoResources.separator + myPort + DynamoResources.separator + hashKey;
                String[] msg = new String[5];
                msg[0] = DynamoResources.COORDINATION;
                msg[1] = coordinatorPort;
                msg[2] = msgToSend;
                msg[3] = ring.getPreferenceList(coordinatorPort);
                msg[4] = key;
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

        if(type.equals(DynamoResources.REPLICATION)) {
            Log.d(DynamoResources.TAG,"Count "+count++ +" Inserting into my DB key " + values.get(DynamoResources.KEY_COL));
            messengerDb = dbHelper.getWritableDatabase();
            Cursor cur = messengerDb.query(DynamoResources.TABLE_NAME, null,
                    DynamoResources.KEY_COL + "='" + values.get(DynamoResources.KEY_COL) + "'", null, null, null, null);
            if (cur.getCount() > 0) {
                cur.moveToFirst();
                int oldVersion = cur.getInt(2);
                int newVersion = values.getAsInteger(DynamoResources.VERSION);
                Log.d(DynamoResources.TAG,"Count "+count++ +" Already present key "+values.get(DynamoResources.KEY_COL)+", Updating with new version "+newVersion);
                if (newVersion > oldVersion) {
                    values.put(DynamoResources.VERSION, newVersion);
//                messengerDb.update(DynamoResources.TABLE_NAME, values, "value = '" + values.get(DynamoResources.VAL_COL) + "'", null);
                    messengerDb.update(DynamoResources.TABLE_NAME, values, DynamoResources.KEY_COL+" = '" + values.get(DynamoResources.KEY_COL) + "'", null);
                    return Math.max(newVersion,oldVersion);
                }
            } else {
                Log.d(DynamoResources.TAG,"Count "+count++ +" Inserting new row with key "+values.get(DynamoResources.KEY_COL));
                long rowId = messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
            }
        }//if(type.equals(DynamoResources.FAILED))
        else {
            messengerDb = dbHelper.getWritableDatabase();
            Cursor cur = messengerDb.query(DynamoResources.TABLE_NAME, null,
                    DynamoResources.KEY_COL + "='" + values.get(DynamoResources.KEY_COL) + "'", null, null, null, null);
            Log.d(DynamoResources.TAG,"Count "+count++ +" Inserting into my DB for "+ type +" key " + values.get(DynamoResources.KEY_COL));
            if (cur.getCount() > 0) {
                cur.moveToFirst();
                Log.d(DynamoResources.TAG,"Count "+count++ +" Count "+cur.getCount()+" Column Count "+cur.getColumnCount()+" Index: "+cur.getColumnName(2));
                int oldVersion = cur.getInt(2);
                Log.d(DynamoResources.TAG,"Count "+count++ +" Have old data for key "+ values.get(DynamoResources.KEY_COL)+"with older version "+ oldVersion);
                if(values.size() == 3)
                    values.remove(DynamoResources.VERSION);
                values.put(DynamoResources.VERSION, oldVersion+1);
                Log.d(DynamoResources.TAG,"Count "+count++ +" Values count "+values.size());
//                values.clear();
//                messengerDb.update(DynamoResources.TABLE_NAME, values, "value = '" + values.get(DynamoResources.VAL_COL) + "'", null);
                messengerDb.update(DynamoResources.TABLE_NAME, values, DynamoResources.KEY_COL+" = '" + values.get(DynamoResources.KEY_COL) + "'", null);
                return oldVersion + 1;

            }
            else {
                values.put(DynamoResources.VERSION, 1);
                Log.d(DynamoResources.TAG,"Count "+count++ +" Inserting new row with key "+values.get(DynamoResources.KEY_COL));
                long rowId = messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
                return 1;
            }
        }

        return 0;
    }

	@Override
	public boolean onCreate() {
        Log.d(DynamoResources.TAG,"Count "+count++ +" Inside Create");
        dbHelper = new GroupMessageDbHelper(getContext(), DynamoResources.DB_NAME, DynamoResources.DB_VERSION, DynamoResources.TABLE_NAME);

        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {

        projection = new String[2];
        projection[0] = DynamoResources.KEY_COL;
        projection[1] = DynamoResources.VAL_COL;
        Log.d(DynamoResources.TAG,"Count "+count++ +" Inside Query");
        messengerDb = dbHelper.getReadableDatabase();
        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
        qBuilder.setTables(DynamoResources.TABLE_NAME);
        Cursor resultCursor = null;
        try {
            if(selection.equals(DynamoResources.SELECTALL))
            {
                resultCursor = qBuilder.query(messengerDb, projection, null,
                        selectionArgs, null, null, sortOrder);
                Log.v(DynamoResources.TAG,"Count "+count++ +" Select * local count: "+resultCursor.getCount());

                if(isRequester) {
                    originator = myPort;
                    MatrixCursor mx = getResults(selection, originator);
                    mx.moveToLast();
                    Log.v(DynamoResources.TAG,"Count "+count++ +" Before Merging Count "+mx.getCount());
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
                    Log.v(DynamoResources.TAG,"Count "+count++ +" After Merging Count " + mx.getCount());
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
                        Log.v(DynamoResources.TAG,"Count "+count++ +" After Merging Count " + mx.getCount());
                        return mx;
                    }
                }
            }
            else if(selection.equals(DynamoResources.SELECTLOCAL))
            {
                Log.v(DynamoResources.TAG,"Count "+count++ +" Query "+selection);
                resultCursor = qBuilder.query(messengerDb, projection, null,
                        selectionArgs, null, null, sortOrder);
                Log.v(DynamoResources.TAG,"Count "+count++ +" Count "+ resultCursor.getCount());
            }
            else if(selection.contains(DynamoResources.SINGLE))
            {
                Log.d(DynamoResources.TAG,"Single query no passing "+selection);
                selection = selection.split(DynamoResources.separator)[0];
                resultCursor = qBuilder.query(messengerDb, null, " key = \'" + selection + "\' ",
                        selectionArgs, null, null, sortOrder);
            }
            else
            {
                Log.v(DynamoResources.TAG,"Count "+count++ +" Query a single key "+selection);
                resultCursor = qBuilder.query(messengerDb, null, " key = \'" + selection + "\' ",
                        selectionArgs, null, null, sortOrder);
                if(resultCursor != null && resultCursor.getCount() <= 0)
                {
                    Log.d(DynamoResources.TAG,"Query when I am not having key "+selection);
                    try {
                        return getResults(selection,myPort);
                    }
                    catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                else
                {
                    Log.d(DynamoResources.TAG,"Column count returned " + resultCursor.getColumnCount()+" "+resultCursor.getColumnNames());
                    resultCursor.moveToFirst();
                    queryCount.put(selection,1);
                    queryKeyVersion.put(selection, Integer.parseInt(resultCursor.getString(2)));
                    queryKeyVal.put(selection,resultCursor.getString(1));
                    return getResults(selection,myPort);
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            Log.v(DynamoResources.TAG,"Count "+count++ +" InterruptedException in Query");
        }
        catch (NoSuchAlgorithmException e)
        {
            e.printStackTrace();
            Log.v(DynamoResources.TAG,"Count "+count++ +" NoSuchAlgorithmException in Query");
        }
		return resultCursor;
	}

    private synchronized MatrixCursor getResults(String selection, String origin) throws InterruptedException, NoSuchAlgorithmException {
        Log.d(DynamoResources.TAG,"Count "+count++ +" Inside GetResults Class for "+ selection);
        MatrixCursor mx = null;
        String coordinator = "";
        if(DynamoResources.SELECTALL.equals(selection)) {
            Log.d(DynamoResources.TAG,"Count "+count++ +" Sending Query to next"+ ring.head.next.port);
            if(ring.getLifeStatus(ring.head.next.port))
                coordinator = ring.head.next.port;
            else
                coordinator = ring.head.next.next.port;

            String[] msg = new String[4];
            msg[0] = DynamoResources.QUERY;
            msg[1] = selection;
            msg[2] = coordinator;
            msg[3] = origin;
            new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
        }
        else
        {
            coordinator = lookUpCoordinator(selection, DynamoResources.genHash(selection));
            if(queryKeyVersion.containsKey(selection))
            {
                String replicators = ring.getPreferenceList(coordinator);
                if(coordinator.equals(myPort))
                {
                    Log.d(DynamoResources.TAG,"Query when I am having key "+selection + " and I am coor");
                    String[] msg = new String[4];
                    msg[0] = DynamoResources.REPLQUERY;
                    msg[1] = selection;
                    msg[2] = replicators;
                    msg[3] = origin;
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
                    Timer time = new Timer();
                    time.schedule(new QueryTask(selection), 2000);
                    timerMap.put(selection, time);
                }
                else
                {
                    Log.d(DynamoResources.TAG,"Query when I am having key "+selection + " and I am not coor");
                    String[] rep = replicators.split(DynamoResources.valSeparator);
                    String[] msg = new String[4];
                    msg[0] = DynamoResources.REPLQUERY;
                    msg[1] = selection;
                    msg[3] = origin;

                    if(rep[0].equals(myPort))
                        msg[2] = coordinator+DynamoResources.valSeparator+rep[1];
                    else
                        msg[2] = coordinator+DynamoResources.valSeparator+rep[0];

                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
                    Timer time = new Timer();
                    time.schedule(new QueryTask(selection), 2000);
                    timerMap.put(selection, time);
                }
            }
            else {

                Log.d(DynamoResources.TAG, "Count " + count++ + " Sending Query to " + coordinator);
                String replicators = ring.getPreferenceList(coordinator);
                String[] msg = new String[4];
                msg[0] = DynamoResources.REPLQUERY;
                msg[1] = selection;
                msg[2] = coordinator + DynamoResources.valSeparator + replicators;
                msg[3] = origin;
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
                Timer time = new Timer();
                time.schedule(new QueryTask(selection), 3000);
                timerMap.put(selection, time);
                queryCount.put(selection, 0);
            }
        }

        Log.d(DynamoResources.TAG,"Count "+count++ +" Waiting...");
        String message = queue.take();
        Log.d(DynamoResources.TAG,"Count "+count++ +" Gotcha Message " + message + " at " + myPort);
        String[] received = message.split(DynamoResources.valSeparator);
        mx = new MatrixCursor(new String[]{DynamoResources.KEY_COL, DynamoResources.VAL_COL}, 50);
        for (String m : received) {
            Log.v(DynamoResources.TAG,"Count "+count++ +" Merging my results");
            String[] keyVal = m.split(" ");
            if(!myKeysMap.containsKey(keyVal[0]))
                mx.addRow(new String[]{keyVal[0], keyVal[1]});
        }
        Log.v(DynamoResources.TAG,"Count "+count++ +" GetResults Count "+mx.getCount());
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
//                    Log.v(DynamoResources.TAG,"Count "+count++ +" Socket Accepted");
                    InputStream read = socket.getInputStream();
                    String msgReceived = "";
                    InputStreamReader reader = new InputStreamReader(read);
                    BufferedReader buffer = new BufferedReader(reader);
                    msgReceived = buffer.readLine();
                    Log.v(DynamoResources.TAG,"Count "+count++ +" Message Received :"+msgReceived);
                    String[] msgs = msgReceived.split(DynamoResources.separator);

                    if(msgs[0].equals(DynamoResources.JOINING))
                    {
                        String myNext = ring.head.next.port;
                        String myNextNext = ring.head.next.next.port;
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Joining starts for "+msgs[1]);
                        String message = null;
                        if(!ring.existsInChain(msgs[1])) {
                            failedCoorMap.put(msgs[1],new Hashtable<String, String>());
                            adjustNode(msgs[1]);
                            Log.d(DynamoResources.TAG,"Size Now "+failedCoorMap.size());
                        }

                        else {

                            Log.d(DynamoResources.TAG,"Count "+count++ +" Coming back from the dead "+msgs[1]);
                            ring.setLifeStatus(msgs[1], true);
                            String myCoord = ring.getPreferenceList(msgs[1]);

                            if(myCoord.split(DynamoResources.valSeparator)[0].equals(myPort))
                            {
                                message = recovery(msgs[1],DynamoResources.REPLICATION);
                                Log.d(DynamoResources.TAG,"Count "+count++ +" Sending Recovery Message to "+msgs[1] + " message "+message);
//                                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.RECOVERY, message, msgs[1], myPort});
                            }
                            else if(myNext.equals(msgs[1]) || myNextNext.equals(msgs[1]))
                            {
                                message = recovery(myPort,"");
                                Log.d(DynamoResources.TAG,"Count "+count++ +" Sending My Keys to "+msgs[1] + " message "+message);
//                                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.RECOVERY, message, msgs[1], myPort});
                            }
                        }
                        if(message != null && !message.equals(""))
                        {
                            new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.HEARTBEAT, msgs[1],message});
                        }
                        else
                        {
                            new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.HEARTBEAT, msgs[1]});
                        }

//                        if(message != null && !message.equals(""))
//                            new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.RECOVERY, message, msgs[1], myPort});
                    }

                    else if(msgs[0].equals(DynamoResources.HEARTBEAT))
                    {
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Heartbeat of " + msgs[1]);
                        failedCoorMap.put(msgs[1],new Hashtable<String, String>());
                        adjustNode(msgs[1]);
                        if(msgs.length > 2) {
                            Log.d(DynamoResources.TAG,"Count "+count++ +" Recovery message by "+msgs[1]);
                            publishProgress(new String[]{DynamoResources.RECOVERY, msgs[2], msgs[1]});
                        }
                    }

                    else if(msgs[0].equals(DynamoResources.COORDINATION))
                    {
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Coordination Message by "+msgs[3]+" for key "+msgs[1]);
                        ContentValues cv = new ContentValues();

                        cv.put(DynamoResources.KEY_COL, msgs[1]);
                        cv.put(DynamoResources.VAL_COL, msgs[2]);
                        int version = myInsert(cv,DynamoResources.COOR);
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Coordination version for key " + msgs[1]+" received: "+version);
                        String msgToSend = DynamoResources.ALIVE + DynamoResources.separator + version+DynamoResources.separator+myPort+
                                DynamoResources.separator +msgs[1];
//                                + DynamoResources.separator +
//                                msgs[2] + DynamoResources.separator + myPort + DynamoResources.separator + msgs[4];
                        String[] msg = new String[3];
                        msg[0] = DynamoResources.ALIVE;
                        msg[1] = msgToSend;
                        msg[2] = msgs[3];
                        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);

//                        myKeySet.add(msgs[1]);
                        myKeysMap.put(msgs[1],msgs[2]);
                        failedVersions.put(msgs[1],version);
                    }

                    else if(msgs[0].equals(DynamoResources.REPLICATION))
                    {
                        int version = 0;
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Replication Message by " + msgs[3] + " for key " + msgs[1] + " with version " + msgs[5]);
                        if(!msgs[5].equals(DynamoResources.FAILED)) {

                            version = Integer.parseInt(msgs[5]);
                            ContentValues cv = new ContentValues();
                            cv.put(DynamoResources.KEY_COL, msgs[1]);
                            cv.put(DynamoResources.VAL_COL, msgs[2]);
                            cv.put(DynamoResources.VERSION, version);
                            myInsert(cv,DynamoResources.REPLICATION);
                            Log.d(DynamoResources.TAG,"Count "+count++ +" Inserted version for key " + msgs[1]+" received: "+version);
                        }
                        else {
//                            Log.d(DynamoResources.TAG,"Count "+count++ +" Replication Message by " + msgs[3] + " for key " + msgs[1] + " with version " + msgs[5]);
                            ContentValues cv = new ContentValues();
                            cv.put(DynamoResources.KEY_COL, msgs[1]);
                            cv.put(DynamoResources.VAL_COL, msgs[2]);
//                            cv.put(DynamoResources.COOR, msgs[6]);

                            version = myInsert(cv,DynamoResources.FAILED);
                            Log.d(DynamoResources.TAG,"Count "+count++ +" version for key " + msgs[1]+" received: "+version);
                        }
                        if(msgs.length >= 7)
                        {
//                            Log.d(DynamoResources.TAG,"Count "+count++ +" Inserting into map for key " + msgs[1] + " for port "+msgs[6] + " key values "+failedCoorMap.entrySet());
                            Hashtable<String, String> dummy = failedCoorMap.get(msgs[6]);
                            dummy.put(msgs[1], msgs[2]);
//                            if(failedCoorMap.contains(msgs[6]))
//                            {
//                                Log.d(DynamoResources.TAG,"Count "+count++ +" Already contains port "+msgs[6]+" key "+msgs[1]);
//                                dummy = failedCoorMap.get(msgs[6]);
//                                dummy.put(msgs[1], msgs[2]);
//                            }
//                            else
//                            {
//                                dummy = new Hashtable<>();
//                                dummy.put(msgs[1], msgs[2]);
//                            }
//                            Log.d(DynamoResources.TAG,msgs[1]+" Current Values for port " + msgs[6]+" are: "+dummy.entrySet().toString());
                            failedCoorMap.put(msgs[6], dummy);
                            failedVersions.put(msgs[1], version);
                        }
                    }
                    else if(msgs [0].equals(DynamoResources.QUERY))
                    {
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Query Request "+myPort+" "+msgs[0]+" "+msgs[1]);
                        originator = msgs[3];
                        isRequester = false;
                        if(mUri == null)
                            mUri = DynamoResources.buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                        publishProgress(new String[]{msgs[0],msgs[1],msgs[2]});
                    }
                    else if(msgs[0].equals(DynamoResources.QUERYREPLY))
                    {
                        if(msgs.length == 3) {
                            Log.d(DynamoResources.TAG,"Count "+count++ +" Got a reply Mate " + msgs[2]);
                            queue.put(msgs[2]);
                        }
                        else if(msgs.length == 4)
                        {
                            int qCount = queryCount.get(msgs[1]);
                            qCount++;
                            String[] received = msgs[2].split(DynamoResources.valSeparator)[0].split(" ");

                            if(queryKeyVersion.containsKey(msgs[1]))
                            {
                                Log.d(DynamoResources.TAG,"Inserting to map when already has key "+received[0]);
                                int version = queryKeyVersion.get(msgs[1]);
                                if(version < Integer.parseInt(received[2])) {
                                    queryKeyVal.put(msgs[1],received[1]);
                                    queryKeyVersion.put(msgs[1], Integer.parseInt(received[2]));
                                }
                            }
                            else {
                                Log.d(DynamoResources.TAG,"Inserting to map when not has key "+received[0]);
                                queryKeyVal.put(msgs[1],received[1]);
                                queryKeyVersion.put(msgs[1],Integer.parseInt(received[2]));
                            }

                            if(qCount == 3)
                            {
                                Log.d(DynamoResources.TAG,"Total count 3 for key "+received[0]);
                                Timer time = timerMap.get(msgs[1]);
                                time.cancel();
                                String message = msgs[1]+" "+queryKeyVal.get(msgs[1])+" "+queryKeyVersion.get(msgs[1])+DynamoResources.valSeparator;
                                Log.d(DynamoResources.TAG,"Generating Message "+message);
                                queue.put(message);
                            }
                            else queryCount.put(msgs[1],qCount);
                        }
                        else
                        {
                            queue.put("");
                        }
                    }
                    else if(msgs[0].equals(DynamoResources.DELETE))
                    {
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Delete request "+msgs[0]+" "+msgs[1]);
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
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Alive message by "+msgs[2]+" for "+msgs[3]);
                        publishProgress(new String[]{msgs[0], msgs[1]} );
//                        insertBlock.clear();
//                        insertBlock.add(msgs[1]);
//                        if(blockMap.contains(msgs[3]))
//                        {
//                            Log.d(DynamoResources.TAG,"Count "+count++ +" inside inside inside");
//                            BlockingQueue<String> bb = blockMap.get(msgs[3]);
//                            bb.add(msgs[1]);
//                            bb.clear();
////                            blockMap.put(msgs[3],bb);
//                            blockMap.remove(msgs[3]);
//                        }

                    }
                    else if(msgs[0].equals(DynamoResources.RECOVERY))
                    {
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Recovery message by "+msgs[1]);
                        if(msgs.length > 2  )
                            publishProgress(new String[]{msgs[0], msgs[2], msgs[1]});
                    }

//                    else if(msgs[0].equals(DynamoResources.REPLQUERY))
//                    {
//
//                    }
                }
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
                Log.e(DynamoResources.TAG,"Count "+count++ +" IOException in ServerTask: "+ex.getMessage());
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
                Log.e(DynamoResources.TAG,"Count "+count++ +" Exception in ServerTask: "+ex.getMessage());
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            super.onProgressUpdate(values);

            if (values[0].equals(DynamoResources.QUERY)) {

                if(values[1].equals(DynamoResources.SELECTALL)) {
                    Cursor resultCursor = query(mUri, null,
                            values[1], null, null);
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Query Requested result count"+resultCursor.getCount()+"");
                    String message = "";
                    if(resultCursor.getCount() > 0)
                        message = DynamoResources.getCursorValue(resultCursor);
                    isRequester = true;
                    Log.v(DynamoResources.TAG,"Count "+count++ +" Now replying back with "+message);
                    String[] msgToRequester = new String[4];
                    msgToRequester[0] = DynamoResources.QUERYREPLY;
                    msgToRequester[1] = values[1];
                    msgToRequester[2] = message;
                    msgToRequester[3] = values[2];
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msgToRequester);
                }
                else
                {
                    Cursor resultCursor = query(mUri, null,
                            values[1]+DynamoResources.separator+DynamoResources.SINGLE, null, null);
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Query Requested result count"+resultCursor.getCount()+"");
                    String message = "";
                    if(resultCursor.getCount() > 0)
                        message = DynamoResources.getCursorValue(resultCursor);
                    isRequester = true;
                    Log.v(DynamoResources.TAG,"Count "+count++ +" Now replying back with "+message);
                    String[] msgToRequester = new String[5];
                    msgToRequester[0] = DynamoResources.QUERYREPLY;
                    msgToRequester[1] = values[1];
                    msgToRequester[2] = message;
                    msgToRequester[3] = values[2];
                    msgToRequester[4] = DynamoResources.SINGLE;
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msgToRequester);
                }
            }
            else if(values[0].equals(DynamoResources.RECOVERY))
            {
                Log.d(DynamoResources.TAG, "Starting Recovery");
                Hashtable<String, String> dummy = failedCoorMap.get(values[2]);
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
                        dummy.put(m[0],m[1]);
                        failedVersions.put(m[0],Integer.parseInt(m[2]));
                        myInsert(cv,DynamoResources.REPLICATION);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
                failedCoorMap.put(values[2],dummy);
            }
            else if(values[0].equals(DynamoResources.ALIVE))
            {
                Log.d(DynamoResources.TAG,"Count "+count++ +" Current queue size: "+insertBlock.size());
                insertBlock.clear();
                try {
                    insertBlock.put(values[1]);
                } catch (InterruptedException e) {
                    Log.e(DynamoResources.TAG,"Count "+count++ +" Interrupted in Publish progress");
                    e.printStackTrace();
                }
            }

//            else if()
        }

    }

    public static class ClientTask extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... params) {

            String msgToSend = "";
            String portToSend = "";
            if (params[0].equals(DynamoResources.JOINING)) {
                msgToSend = params[0] + DynamoResources.separator + params[1];
                for (String port : remotePorts.keySet()) {
                    if (!port.equals(myPort))
                        sendMessage(port, msgToSend);
                }
            } else if (params[0].equals(DynamoResources.HEARTBEAT)) {
                if(params.length == 3)
                    msgToSend = params[0] + DynamoResources.separator + myPort + DynamoResources.separator + params[2];
                else
                    msgToSend = params[0] + DynamoResources.separator + myPort;
                portToSend = params[1];
                sendMessage(portToSend, msgToSend);
            } else if (params[0].equals(DynamoResources.COORDINATION)) {
                portToSend = params[1];
                sendMessage(portToSend, DynamoResources.COORDINATION + DynamoResources.separator + params[2]);
//                BlockingQueue<String> bb = new SynchronousQueue<>();
//                blockMap.put(params[4], bb);
                String[] replicators = params[3].split(DynamoResources.valSeparator);
                if (ring.getLifeStatus(portToSend)) {
                    try {
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Waiting for version...by " + portToSend + " for " + params[4]);
                        String version = insertBlock.poll(2000, TimeUnit.MILLISECONDS);
//                        version = blockMap.get(params[4]).poll(3000, TimeUnit.MILLISECONDS);
                        if (version != null) {
                            Log.d(DynamoResources.TAG,"Count "+count++ +" Coordinator replied with version " + version + " by  " + portToSend + " for " + params[4]);
                            msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + params[2] +
                                    DynamoResources.separator + version + DynamoResources.separator + portToSend;
                        } else {
                            Log.d(DynamoResources.TAG,"Count "+count++ +" Time out for key "+params[4]);
                            ring.setLifeStatus(portToSend, false);
                            msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + params[2] +
                                    DynamoResources.separator + DynamoResources.FAILED
                                    + DynamoResources.separator + portToSend;
                        }

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Replication when coordinator is dead "+portToSend);
                    msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + params[2] +
                            DynamoResources.separator + DynamoResources.FAILED + DynamoResources.separator + portToSend;
                }

//                msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + params[2];
                sendMessage(replicators[0], msgToSend);
                sendMessage(replicators[1], msgToSend);
            } else if (params[0].equals(DynamoResources.REPLICATION)) {
                msgToSend = params[1];
                sendMessage(ring.head.next.port, msgToSend);
                sendMessage(ring.head.next.next.port, msgToSend);// + DynamoResources.separator + "2"
            } else if (params[0].equals(DynamoResources.QUERY)) {
                msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + myPort + DynamoResources.separator + params[3];
                portToSend = params[2];
                sendMessage(portToSend, msgToSend);
            } else if (params[0].equals(DynamoResources.QUERYREPLY)) {
                if(params.length == 4)
                    msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + params[2];
                else
                    msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + params[2] + DynamoResources.separator + params[4];
                portToSend = params[3];
                sendMessage(portToSend, msgToSend);
            } else if (params[0].equals(DynamoResources.DELETE)) {
                msgToSend = params[0] + DynamoResources.separator + params[1];
                portToSend = params[2];
                sendMessage(portToSend, msgToSend);
            } else if (params[0].equals(DynamoResources.ALIVE)) {
                msgToSend = params[1];
                portToSend = params[2];
                sendMessage(portToSend, msgToSend);
            } else if (params[0].equals(DynamoResources.RECOVERY)) {
                Log.d(DynamoResources.TAG,"Count "+count++ +" Sending recovery message to "+params[2]);
                msgToSend = DynamoResources.RECOVERY + DynamoResources.separator + params[3] + DynamoResources.separator + params[1];
                portToSend = params[2];
                sendMessage(portToSend, msgToSend);
            }

            else if(params[0].equals(DynamoResources.REPLQUERY))
            {
                String[] senders = params[2].split(DynamoResources.valSeparator);
                msgToSend = DynamoResources.QUERY + DynamoResources.separator + params[1] + DynamoResources.separator +
                        myPort + DynamoResources.separator + params[3] + DynamoResources.separator + DynamoResources.SINGLE;

                for(String sender : senders)
                    sendMessage(sender,msgToSend);
            }
            return null;
        }

        private synchronized static void sendMessage(String portToSend, String msgToSend) {
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

            } catch (UnknownHostException e) {
                e.printStackTrace();
                Log.e(DynamoResources.TAG,"Count "+count++ +" ClientTask UnknownHostException");
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(DynamoResources.TAG,"Count "+count++ +" ClientTask IOException for " + portToSend);
            } catch (Exception e) {
                e.printStackTrace();
                Log.e(DynamoResources.TAG,"Count "+count++ +" ClientTask Exception");
            }
        }
    }

    private static void adjustNode(String port) throws NoSuchAlgorithmException{
        String hash = DynamoResources.genHash(remotePorts.get(port));
        Node node = ring.head;
        while(true) {

            if (node.next == node && node.previous == node) {
                ring.addNode(node, port, hash, DynamoResources.NEXT);

                Log.d(DynamoResources.TAG,"Count "+count++ +" Current Ring: " + ring.printRing());
                break;
            }

            else {
                if ((hash.compareTo(hashedPort) > 0 && hash.compareTo(node.next.hashPort) < 0) ||
                        (hashedPort.compareTo(node.next.hashPort) > 0 && (hash.compareTo(hashedPort) > 0 || node.next.hashPort.compareTo(hash) > 0))) {
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Adding my next " + port);
                    ring.addNode(node, port, hash, DynamoResources.NEXT);

                    Log.d(DynamoResources.TAG,"Count "+count++ +" Current Ring: " + ring.printRing());
                    break;
                }
                else if ((hash.compareTo(hashedPort) < 0 && hash.compareTo(node.previous.hashPort) > 0) ||
                        (node.previous.hashPort.compareTo(hashedPort) > 0 && (hash.compareTo(node.previous.hashPort) > 0 || hash.compareTo(hashedPort) < 0))) {
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Adding my previous " +port);
                    ring.addNode(node, port, hash, DynamoResources.PREVIOUS);

                    Log.d(DynamoResources.TAG,"Count "+count++ +" Current Ring: " + ring.printRing());
                    break;
                }
                else {
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Sending to next " + port);
                    node = node.next;
                }
            }
        }
    }

    public static void startUp(String myPortNum)
    {
        Log.d(DynamoResources.TAG,"Count "+count++ +" Inside Start Up");
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
            Log.d(DynamoResources.TAG,"Count "+count++ +" Looking up where to store the "+key);

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

    private String recovery(String port, String type)
    {
        Log.d(DynamoResources.TAG,"Count "+count++ +" Generating Recovery message "+failedCoorMap.keySet().toString() +" port "+port);
        String result = "";
        if(type.equals(DynamoResources.REPLICATION)) {
            Log.d(DynamoResources.TAG,"Count "+count++ +" Inside");
//            if (failedCoorMap.contains(port)) {
//                Log.d(DynamoResources.TAG,"Count "+count++ +" It has some keys for " + port);
//                Hashtable<String, String> dummy = failedCoorMap.get(port);
//                for (Map.Entry<String, String> entry : dummy.entrySet()) {
//                    String key = entry.getKey();
//                    String value = entry.getValue();
//                    Log.d(DynamoResources.TAG,"Count "+count++ +" Adding key " + key + "and value " + value);
//                    result = result + key + " " + value + " " + failedVersions.get(key) + DynamoResources.valSeparator;
//                }
////            failedCoorMap.put(port, new Hashtable<String, String>());
//            }
            Log.d(DynamoResources.TAG,"Count "+count++ +" It has some keys for " + port);
            Hashtable<String, String> dummy = failedCoorMap.get(port);
            for (Map.Entry<String, String> entry : dummy.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                Log.d(DynamoResources.TAG,"Count "+count++ +" Adding key " + key + "and value " + value);
                result = result + key + " " + value + " " + failedVersions.get(key) + DynamoResources.valSeparator;
            }
        }
        else {
            for (Map.Entry<String, String> entry : myKeysMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                Log.d(DynamoResources.TAG,"Count "+count++ +" Adding key " + key + "and value " + value);
                result = result + key + " " + value + " " + failedVersions.get(key) + DynamoResources.valSeparator;
            }
        }
        return result;
    }

    public class QueryTask extends TimerTask {

        String key = "";
        public QueryTask(String key)
        {
            this.key = key;
        }

        @Override
        public void run() {
            Log.d(DynamoResources.TAG,"Time out for key "+key);
            if(queryKeyVersion.containsKey(key))
            {
                try {
                    if(queue.isEmpty())
                        queue.put(key+" "+queryKeyVal.get(key)+" "+queryKeyVersion.get(key) + DynamoResources.valSeparator);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}

//Coordination and Replication message need not to have hashkey. Look into this. Remove if not needed till the final submission.
//Query functionality for a single key. Send request to coordinator directly.
//* queries should return the unique values.