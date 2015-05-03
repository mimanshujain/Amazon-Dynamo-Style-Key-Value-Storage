package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
    private static SQLiteDatabase messengerDb;
    private static Uri mUri = null;
    //A reference for Db Helper Class
    private static GroupMessageDbHelper dbHelper;
    //Threading Attributes
    private static final BlockingQueue<Runnable> sPoolWorkQueue =
            new LinkedBlockingQueue<Runnable>(2000);
    static final Executor myPool = new ThreadPoolExecutor(2000,3000,1, TimeUnit.SECONDS,sPoolWorkQueue);
    //Other Variables
    static String myPort = "";
    static String hashedPort = "";
    static boolean isRequester = true;
    private static BlockingQueue<String> queue = new SynchronousQueue<>();
    private static BlockingQueue<String> insertBlock = new SynchronousQueue<>();
    private static BlockingQueue<String> queryBlock = new SynchronousQueue<>();
    static ChordLinkList ring = null;
    static ChordLinkList fixRing = null;
    static String originator = "";

    //Maps and Collections
    static Hashtable<String,String> remotePorts = new Hashtable<>();
    static Hashtable<String, Hashtable<String, String>> failedCoorMap = new Hashtable<>();
    static Hashtable<String, Integer> failedVersions = new Hashtable<>();
    static Hashtable<String, String> myKeysMap = new Hashtable<>();
    static Hashtable<String, Integer> queryCount = new Hashtable<>();
    static Hashtable<String, String> queryKeyVal = new Hashtable<>();
    static Hashtable<String, Integer> queryKeyVersion = new Hashtable<>();
    static Hashtable<String, Timer> timerMap = new Hashtable<>();
    static Hashtable<String, Hashtable<String, Boolean>> keyStat = new Hashtable<>();
    static Hashtable<String, Integer> keyLockMap = new Hashtable<>();
    static Hashtable<String, Integer> insertMap = new Hashtable<>();
    static Hashtable<String, String> queryAll = new Hashtable<>();

    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        messengerDb = dbHelper.getWritableDatabase();
        Log.v(DynamoResources.TAG,"Count "+count++ +" Delete at "+myPort +" Key= "+selection);
        String select = "\"*\"";
        Log.v(selection.equals(select)+""," Select");
        int num = 0;

        if(selection.equals(DynamoResources.SELECTLOCAL)) {
            Log.d(DynamoResources.TAG,"Delete my all local request");
            num = messengerDb.delete(DynamoResources.TABLE_NAME, null, null);
            myKeysMap = new Hashtable<>();
        }
        else if(selection.equals(DynamoResources.SELECTALL))
        {
            num = messengerDb.delete(DynamoResources.TABLE_NAME, null, null);
            myKeysMap = new Hashtable<>();
            failedCoorMap = new Hashtable<>();
            String[] msg = new String[3];
            msg[0] = DynamoResources.DELETE;
            msg[1] = myPort;
            msg[2] = ring.getLifeStatus(ring.head.next.port)?ring.head.next.port:ring.head.next.next.port;
            new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
        }
        else if(selection.contains(DynamoResources.SINGLE))
        {
            selection = selection.split(DynamoResources.separator)[0];
            Log.d(DynamoResources.TAG,"Sinle Single I am deleting the key "+selection);
            num = messengerDb.delete(DynamoResources.TABLE_NAME,"key = \'"+selection+"\'",null);
        }
        else
        {
            try {
                String hashKey = DynamoResources.genHash(selection);
                String coordinatorPort = lookUpCoordinator(selection, hashKey);
                String[] replicators = ring.getPreferenceListArray(coordinatorPort);
                Log.d(DynamoResources.TAG,"Found coordinator is "+coordinatorPort);
                if(coordinatorPort.equals(myPort))
                {
                    Log.d(DynamoResources.TAG,"I am deleting the key "+selection);
                    num = messengerDb.delete(DynamoResources.TABLE_NAME,"key = \'"+selection+"\'",null);
                    myKeysMap.remove(selection);
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool,
                            new String[]{
                                    DynamoResources.DELETE,
                                    selection + DynamoResources.valSeparator + DynamoResources.SINGLE,
                                    replicators[0] + DynamoResources.valSeparator + replicators[1]
                            });
//                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.DELETE,selection
//                            +DynamoResources.separator+DynamoResources.SINGLE,replicators[1]});
                }
                else {
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.DELETE,selection
                            +DynamoResources.valSeparator+DynamoResources.SINGLE,coordinatorPort+DynamoResources.valSeparator+replicators[0]+DynamoResources.valSeparator+replicators[1]});
//                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.DELETE,selection
//                            +DynamoResources.separator+DynamoResources.SINGLE,replicators[0]});
//                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.DELETE,selection
//                            +DynamoResources.separator+DynamoResources.SINGLE,replicators[1]});
                }
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
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

            if(!keyLockMap.containsKey(key))
                keyLockMap.put(key,2);
            Log.d(DynamoResources.TAG,"Checking the lock ");
            while(keyLockMap.get(key) == 1){Log.d(DynamoResources.TAG,"Waiting for lINSERT ock for "+key+"  .......................");}
            Log.d(DynamoResources.TAG,"Lock is free now and locking it....");
            keyLockMap.put(key, 1);

            Log.d(DynamoResources.TAG,"Count "+count++ +" Insertion Starts for key "+key +" with value "+value);
            String hashKey = DynamoResources.genHash(values.get(DynamoResources.KEY_COL).toString());

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
                Log.d(DynamoResources.TAG,"Count "+count++ +" My Key "+key);
                messengerDb = dbHelper.getWritableDatabase();
                Cursor cur = messengerDb.query(DynamoResources.TABLE_NAME, null,
                        DynamoResources.KEY_COL + "='" + values.get(DynamoResources.KEY_COL) + "'", null, null, null, null);

                if (cur.getCount() > 0) {
                    cur.moveToFirst();
                    Log.d(DynamoResources.TAG,"Count "+count++ +DynamoResources.separator+" When I already had the key "+key);
                    int oldVersion = cur.getInt(2);
                    currentVersion = oldVersion + 1;
                    if(values.size() == 3)
                        values.remove(DynamoResources.VERSION);
                    values.put(DynamoResources.VERSION,currentVersion);
                    messengerDb.update(DynamoResources.TABLE_NAME, values, DynamoResources.KEY_COL+" = '" + values.get(DynamoResources.KEY_COL) + "'", null);
                }
                else
                {
                    Log.d(DynamoResources.TAG,"Count "+count++ +DynamoResources.separator+" I am freshly inserting key "+key);
                    currentVersion = 1;
                    if(values.size() == 3)
                        values.remove(DynamoResources.VERSION);
                    values.put(DynamoResources.VERSION, currentVersion);
                    messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
                }
                insertMap.put(key,1);
                myKeysMap.put(key,value);
                failedVersions.put(key,currentVersion);

                String msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + key + DynamoResources.separator +
                        value + DynamoResources.separator + myPort;

                if(ring.size() == 5) {
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.REPLICATION, msgToSend,ring.head.next.port,key});
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.REPLICATION, msgToSend,ring.head.next.next.port,key});
                }
                else
                {
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.REPLICATION, msgToSend,fixRing.head.next.port,key});
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.REPLICATION, msgToSend,fixRing.head.next.next.port,key});
                }

            }

            else {
                Log.d(DynamoResources.TAG,"Count "+count++ +" Sending Key "+key+" to real Coordinator and replicators "+coordinatorPort);
                String msgToSend = key + DynamoResources.separator +
                        value + DynamoResources.separator + coordinatorPort;

                insertMap.put(key,0);
                String[] replicators;
                if(ring.size() == 5) replicators = ring.getPreferenceListArray(coordinatorPort);
                else replicators = fixRing.getPreferenceListArray(coordinatorPort);

                String replPort = "";
                boolean toDouble = true;
                if(replicators[0].equals(myPort))
                {
                    toDouble = false;
                    replPort = replicators[1];
                }
                if(replicators[1].equals(myPort))
                {
                    toDouble = false;
                    replPort = replicators[0];
                }

                //Sending to the actual coordinator
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool,  new String[]{DynamoResources.COORDINATION,
                        DynamoResources.COORDINATION + DynamoResources.separator + msgToSend,coordinatorPort,key});

                //If need to send to only 1
                if(!toDouble)
                {
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool,  new String[]{DynamoResources.REPLICATION,
                            DynamoResources.REPLICATION + DynamoResources.separator + msgToSend,replPort,key});
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Inserting into my DB key " + values.get(DynamoResources.KEY_COL)+" Replicator");
                    if(myInsert(values,DynamoResources.REPLICATION) != 0)
                    {
                        if(insertMap.containsKey(key)) {
                            insertMap.put(key, insertMap.get(key) + 1);
                            if(insertMap.get(key) == 2) keyLockMap.put(key,2);
                        }

                        if(failedCoorMap.containsKey(coordinatorPort)) {
                            Hashtable<String, String> dummy = failedCoorMap.get(coordinatorPort);
                            dummy.put(key,value);
                            failedCoorMap.put(coordinatorPort,dummy);
                        }
                        else
                        {
                            Hashtable<String, String> dummy = new Hashtable<>();
                            dummy.put(key,value);
                            failedCoorMap.put(coordinatorPort,dummy);
                        }
                    }
                }
                else
                {
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.REPLICATION,
                            DynamoResources.REPLICATION + DynamoResources.separator + msgToSend, replicators[0], key});
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.REPLICATION,
                            DynamoResources.REPLICATION + DynamoResources.separator + msgToSend, replicators[1], key});
                }
            }
//            insertMap.remove(key);
        }
        catch (NoSuchAlgorithmException ex)
        {
            ex.printStackTrace();
            Log.e(DynamoResources.TAG,ex.getMessage());
        }
		return null;
	}

    private static synchronized int myInsert(ContentValues values, String type) throws NoSuchAlgorithmException{

        messengerDb = dbHelper.getWritableDatabase();
        Cursor cur = messengerDb.query(DynamoResources.TABLE_NAME, null,
                DynamoResources.KEY_COL + "='" + values.get(DynamoResources.KEY_COL) + "'", null, null, null, null);

        if(type.equals(DynamoResources.REPLICATION))
        {
            Log.d(DynamoResources.TAG,"Count "+count++ +" "+type+" into my DB key " + values.get(DynamoResources.KEY_COL));
            if (cur.getCount() > 0) {
                cur.moveToFirst();
                int oldVersion = cur.getInt(2);
                Log.d(DynamoResources.TAG,"Count "+count++ +" Have old data for key "+ values.get(DynamoResources.KEY_COL)+" with older version "+ oldVersion);
                if(values.size() == 3)
                    values.remove(DynamoResources.VERSION);
                values.put(DynamoResources.VERSION, oldVersion+1);
                messengerDb.update(DynamoResources.TABLE_NAME, values, DynamoResources.KEY_COL+" = '" + values.get(DynamoResources.KEY_COL) + "'", null);
                failedVersions.put(values.get(DynamoResources.KEY_COL).toString(),oldVersion + 1);
                return oldVersion + 1;
            }
            else {
                Log.d(DynamoResources.TAG,"Count "+count++ +" Inserting new row with key "+values.get(DynamoResources.KEY_COL));
                long rowId = messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
                failedVersions.put(values.get(DynamoResources.KEY_COL).toString(),1);
                return 1;
            }
        }

        else if(type.equals(DynamoResources.RECOVERY))
        {
            Log.d(DynamoResources.TAG,"Count "+count++ +" "+type+" into my DB key " + values.get(DynamoResources.KEY_COL));
            if (cur.getCount() > 0) {
                cur.moveToFirst();
                int oldVersion = cur.getInt(2);
                int newVersion = values.getAsInteger(DynamoResources.VERSION);
                if(newVersion > oldVersion)
                {
                    messengerDb.update(DynamoResources.TABLE_NAME, values, DynamoResources.KEY_COL+" = '" + values.get(DynamoResources.KEY_COL) + "'", null);
                    failedVersions.put(values.get(DynamoResources.KEY_COL).toString(),oldVersion + 1);
                }

                return newVersion > oldVersion?newVersion:oldVersion;
            }
            else {
                Log.d(DynamoResources.TAG,"Count "+count++ +" Inserting new row with key "+values.get(DynamoResources.KEY_COL));
                long rowId = messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
                failedVersions.put(values.get(DynamoResources.KEY_COL).toString(),1);
                return 1;
            }
        }
        return  0;
    }

	@Override
	public boolean onCreate() {
        Log.d(DynamoResources.TAG,"Count "+count++ +" Inside Create");
        dbHelper = new GroupMessageDbHelper(getContext(), DynamoResources.DB_NAME, DynamoResources.DB_VERSION, DynamoResources.TABLE_NAME);
        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.FAILED});
        try
        {
            Log.d(DynamoResources.TAG,"Starting Server");
            ServerSocket socket = new ServerSocket(10000);
            new ServerTask().executeOnExecutor(SimpleDynamoProvider.myPool, socket);
        }
        catch (IOException ex)
        {
            Log.v(DynamoResources.TAG, "Server Socket creation Error");
        }
        return false;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {

        Log.d(DynamoResources.TAG,"Count "+count++ +" Inside Query for "+ selection);
        messengerDb = dbHelper.getReadableDatabase();
        SQLiteQueryBuilder qBuilder = new SQLiteQueryBuilder();
        qBuilder.setTables(DynamoResources.TABLE_NAME);
        Cursor resultCursor = null;

        if(!keyLockMap.containsKey(selection))
            keyLockMap.put(selection,2);
        Log.d(DynamoResources.TAG,"Checking the lock ");
        while(keyLockMap.get(selection) == 1){Log.d(DynamoResources.TAG,"Waiting for lINSERT ock for "+selection+"  .......................");}
        Log.d(DynamoResources.TAG,"Lock is free now and locking it........................................................");
        keyLockMap.put(selection, 1);

        try {
            if(selection.equals(DynamoResources.SELECTALL))
            {
                resultCursor = qBuilder.query(messengerDb, null, null,
                        selectionArgs, null, null, null);
                Log.v(DynamoResources.TAG,"Count "+count++ +" Select * local count: "+resultCursor.getCount()+" Colum count "+resultCursor.getColumnCount());
                MatrixCursor mx = getResults(selection, originator);
                if(mx == null || mx.getCount() <= 0) return resultCursor;
                mx.moveToLast();
                if (resultCursor.getCount() > 0) {
                    resultCursor.moveToFirst();
                    String cursorStr = DynamoResources.getCursorValue(resultCursor);
                    String[] received = cursorStr.split(DynamoResources.valSeparator);
                    for (String m : received) {
                        String[] keyVal = m.split(" ");
//                            mx.addRow(new String[]{keyVal[0], keyVal[1], keyVal[2]});
                        mx.addRow(new String[]{keyVal[0], keyVal[1]});
                    }
                }
                Log.d(DynamoResources.TAG,"Setting lock free now");
                keyLockMap.put(selection, 2);
                return mx;
            }
            else if(selection.equals(DynamoResources.SELECTLOCAL))
            {
                Log.v(DynamoResources.TAG,"Count "+count++ +" Query "+selection);

                resultCursor = qBuilder.query(messengerDb, new String[]{DynamoResources.KEY_COL,DynamoResources.VAL_COL}, null,selectionArgs, null, null, null);

                Log.v(DynamoResources.TAG,"Count "+count++ +" Count "+ resultCursor.getCount());
                keyLockMap.put(selection,2);
            }
            else if(selection.equals(DynamoResources.SELECTMYLOCAL))
            {
                Log.v(DynamoResources.TAG,"Count "+count++ +" Query "+selection);
                resultCursor = qBuilder.query(messengerDb, null, null,
                        selectionArgs, null, null, null);
                keyLockMap.put(selection,2);
            }
            else if(sortOrder != null && sortOrder.equals(DynamoResources.SINGLE))
            {
                Log.d(DynamoResources.TAG,"Single query no passing "+selection);
                resultCursor = qBuilder.query(messengerDb, null, " key = \'" + selection + "\' ",
                        selectionArgs, null, null, null);
                Log.d(DynamoResources.TAG,"Setting lock free now");
                keyLockMap.put(selection,2);
            }
            else
            {
                Log.v(DynamoResources.TAG,"Count "+count++ +" Query a single key "+selection);
                resultCursor = qBuilder.query(messengerDb, null, " key = \'" + selection + "\' ",
                        selectionArgs, null, null, null);

                if(resultCursor.getCount() > 0)
                {
                    Log.d(DynamoResources.TAG,"Column count returned " + resultCursor.getColumnCount()+" "+resultCursor.getColumnNames());
                    resultCursor.moveToFirst();
                    queryCount.put(selection,1);
                    queryKeyVersion.put(selection, Integer.parseInt(resultCursor.getString(2)));
                    queryKeyVal.put(selection,resultCursor.getString(1));
                }

                MatrixCursor mx = getResults(selection,myPort);
                keyLockMap.put(selection,2);
                Log.d(DynamoResources.TAG,"Setting lock free now");
                return mx;
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

        Log.d(DynamoResources.TAG,"Returning "+resultCursor.getCount()+" results. If grader giving missing key. Not my fault.");
		return resultCursor;
	}

    private synchronized MatrixCursor getResults(String selection, String origin) throws InterruptedException, NoSuchAlgorithmException {
        Log.d(DynamoResources.TAG,"Count "+count++ +" Inside GetResults Class for "+ selection);
        MatrixCursor mx = null;
        String coordinator = "";
        if(DynamoResources.SELECTALL.equals(selection))
        {

            for(String port : remotePorts.keySet())
            {
                if(!port.equals(myPort))
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,port,myPort,DynamoResources.SELECTLOCAL});
            }
        }
        else
        {
            coordinator = lookUpCoordinator(selection, DynamoResources.genHash(selection));
            Log.v(DynamoResources.TAG,"The coordinator is "+coordinator);
            if(queryKeyVersion.containsKey(selection))
            {
                String[] replicators;
                if(ring.size() == 5) replicators = ring.getPreferenceListArray(coordinator);
                else replicators = fixRing.getPreferenceListArray(coordinator);
                if(coordinator.equals(myPort))
                {
                    Log.d(DynamoResources.TAG,"Query when I am having key "+selection + " and I am coor");
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[0],myPort,DynamoResources.SINGLE});
//                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[1],myPort,DynamoResources.SINGLE});
                }
                else
                {
                    Log.d(DynamoResources.TAG,"Query when I am having key "+selection + " and I am not coor");
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,coordinator,myPort,DynamoResources.SINGLE});

                    if(replicators[0].equals(myPort))
                        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[1],myPort,DynamoResources.SINGLE});
                    else
                        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[0],myPort,DynamoResources.SINGLE});
                }
            }
            else {
                queryCount.put(selection, 0);
                String[] replicators;
                if(ring.size() == 5) replicators = ring.getPreferenceListArray(coordinator);
                else replicators = fixRing.getPreferenceListArray(coordinator);
                Log.d(DynamoResources.TAG, "Count " + count++ + " Sending Query to " + coordinator +" "+replicators.toString());
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,coordinator,myPort,DynamoResources.SINGLE});
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[0],myPort,DynamoResources.SINGLE});
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[1],myPort,DynamoResources.SINGLE});
            }
//            while(queryCount.get(selection) < 2) {}
        }

        Log.d(DynamoResources.TAG,"Count "+count++ +" Waiting...");
        String message = queue.take();
        Log.d(DynamoResources.TAG,"Count "+count++ +" Gotcha Message " + message + " at " + myPort);
        String[] received = message.split(DynamoResources.valSeparator);
        mx = new MatrixCursor(new String[]{DynamoResources.KEY_COL, DynamoResources.VAL_COL}, 150);
//        mx = new MatrixCursor(new String[]{DynamoResources.KEY_COL, DynamoResources.VAL_COL , DynamoResources.VERSION}, 150);
        for (String m : received) {
            Log.v(DynamoResources.TAG,"Count "+count++ +" Merging my results");
            String[] keyVal = m.split(" ");
//            if(!myKeysMap.containsKey(keyVal[0]))
            mx.addRow(new String[]{keyVal[0], keyVal[1]});
        }

        if(mx.getCount() == 1) {
            mx.moveToFirst();
            Log.d(DynamoResources.TAG, "GetResults Returning for key " + selection + " " + mx.getCount() + " value " + mx.getString(1));
        }

//        keyLockMap.put(selection,2);
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
                    Log.d(DynamoResources.TAG,"Inside Server");
                    Socket socket = serverSocket.accept();
//                    Log.v(DynamoResources.TAG,"Count "+count++ +" Socket Accepted");
                    InputStream read = socket.getInputStream();
                    String msgReceived = "";
//                    InputStreamReader reader = new InputStreamReader(read);
//                    BufferedReader buffer = new BufferedReader(reader);
//                    msgReceived = buffer.readLine();

                    ObjectInputStream in = new ObjectInputStream(read);
                    msgReceived = (String)in.readObject();
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//                    out.writeObject(new String("Mimanshu"));
                    Log.v(DynamoResources.TAG,"Count "+count++ +" Message Received :"+msgReceived);
                    String[] msgs = msgReceived.split(DynamoResources.separator);

                    if(msgs[0].equals(DynamoResources.JOINING))
                    {
                        String myNext = ring.head.next.port;
                        String myNextNext = ring.head.next.next.port;
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Joining starts for "+msgs[1]);
                        String message = null;
                        if(!ring.existsInChain(msgs[1])) {
                            out.writeObject(new String(DynamoResources.OK));
                            out.flush();
                            if(!failedCoorMap.containsKey(msgs[1]))
                                failedCoorMap.put(msgs[1],new Hashtable<String, String>());
                            adjustNode(msgs[1]);
                            Log.d(DynamoResources.TAG,"Size Now "+failedCoorMap.size()+ " Keyset "+failedCoorMap.keySet().toString());
                        }

                        else
                        {
                            Log.d(DynamoResources.TAG,"Count "+count++ +" Coming back from the dead "+msgs[1]);
                            ring.setLifeStatus(msgs[1], true);
                            String myCoord = fixRing.getPreferenceList(msgs[1]);
                            Log.d(DynamoResources.TAG,"Resurrector Pref list "+myCoord);
                            //When I am next to the coordinator. Sending it to its keys.
                            if(myCoord.split(DynamoResources.valSeparator)[0].equals(myPort))
                            {
                                message = recovery(msgs[1],DynamoResources.REPLICATION);
                                Log.d(DynamoResources.TAG,"Count "+count++ +" Sending Recovery Message to "+msgs[1] + " message "+message);
//                                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.RECOVERY, message, msgs[1], myPort});
                                if(message != null && !message.equals(""))
                                    out.writeObject(DynamoResources.COOR+DynamoResources.separator+
                                        message);
                                else
                                    out.writeObject(new String(DynamoResources.OK));
                            }
                            //When my replicator is coming back
                            else if(myNext.equals(msgs[1]) || myNextNext.equals(msgs[1]))
                            {
                                message = recovery(myPort,"");
                                if(message != null && !message.equals(""))
                                    out.writeObject(DynamoResources.REPLICATION+DynamoResources.separator+
                                            message);
                                else
                                    out.writeObject(new String(DynamoResources.OK));
                                Log.d(DynamoResources.TAG,"Count "+count++ +" Sending My Keys to "+msgs[1] + " message "+message);
//                                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.RECOVERY, message, msgs[1], myPort});
                            }
                            else
                                out.writeObject(new String(DynamoResources.OK));
                            out.flush();
                        }
                    }

                    else if(msgs[0].equals(DynamoResources.HEARTBEAT))
                    {
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Heartbeat of " + msgs[1]);
                        if(!failedCoorMap.containsKey(msgs[1]))
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
                        cv.put(DynamoResources.VERSION, 1);
                        if(myInsert(cv,DynamoResources.REPLICATION) != 0)
                            out.writeObject(new String(DynamoResources.OK));
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Coordination version for key " + msgs[1]);
                        myKeysMap.put(msgs[1], msgs[2]);
                    }

                    else if(msgs[0].equals(DynamoResources.REPLICATION))
                    {
                        int version = 1;
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Replication Message by " + msgs[3] + " for key " + msgs[1]);
                        ContentValues cv = new ContentValues();
                        cv.put(DynamoResources.KEY_COL, msgs[1]);
                        cv.put(DynamoResources.VAL_COL, msgs[2]);
                        cv.put(DynamoResources.VERSION, version);
                        if(myInsert(cv,DynamoResources.REPLICATION) != 0)
                            out.writeObject(new String(DynamoResources.OK));
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Inserted version for key " + msgs[1]);

                        Hashtable<String, String> dummy = null;
                        if(failedCoorMap.containsKey(msgs[3]))
                        {
                            Log.d(DynamoResources.TAG,"Count "+count++ +" Already contains port "+msgs[3]+" key "+msgs[1]);
                            dummy = failedCoorMap.get(msgs[3]);
                            dummy.put(msgs[1], msgs[2]);
                        }
                        else
                        {
                            Log.d(DynamoResources.TAG,"Count "+count++ +" Not contains port "+msgs[3]+" key "+msgs[1]);
                            dummy = new Hashtable<>();
                            dummy.put(msgs[1], msgs[2]);
                        }
                        failedCoorMap.put(msgs[3], dummy);
                    }

                    else if(msgs [0].equals(DynamoResources.QUERY))
                    {
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Query Request "+myPort+" "+msgs[0]+" "+msgs[1]);
//                        originator = msgs[3];
//                        isRequester = false;
                        if(mUri == null)
                            mUri = DynamoResources.buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                        Cursor resultCursor = null;
                        if(msgs[4].equals(DynamoResources.SELECTLOCAL))
                        {
                            resultCursor = query(mUri, null,
                                    DynamoResources.SELECTMYLOCAL, null, null);
                        }
                        else
                        {
                            messengerDb = dbHelper.getReadableDatabase();
                            resultCursor = messengerDb.query(true,DynamoResources.TABLE_NAME,null,"key = '"+msgs[1]+"'",null,null,null,null,null);
//                            resultCursor = query(mUri, null,
//                                   msgs[1], null, DynamoResources.SINGLE);
                        }

                        if(resultCursor.getCount() > 0 && resultCursor.getColumnCount() == 3) {
                            resultCursor.moveToFirst();
                            String message = DynamoResources.getCursorValue(resultCursor);
                            isRequester = true;
                            Log.v(DynamoResources.TAG, "Count " + count++ + " Now replying back with " + message);
                            out.writeObject(new String(message));
                            out.flush();
                        }
                        else
                        {
                            out.writeObject(new String(""));
                            out.flush();
                        }
//                        publishProgress(new String[]{msgs[0],msgs[1],msgs[2]});
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
                                Log.d(DynamoResources.TAG,"Inserting to map when already has key "+received[0]+" "+msgs[2]);
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

                        if(msgs[1].contains(DynamoResources.SINGLE))
                        {
                            String selection = msgs[1].split(DynamoResources.valSeparator)[0];
                            Log.v(DynamoResources.TAG,"Deleting a single key "+selection);
//                            delete(mUri,selection,null);
                            messengerDb = dbHelper.getWritableDatabase();
                            messengerDb.delete(DynamoResources.TABLE_NAME,"key = \'"+selection+"\'",null);
                            out.writeObject(new String(DynamoResources.OK));
                            out.flush();
                        }

                        else if(!msgs[1].equals(myPort))
                        {
                            delete(mUri,DynamoResources.SELECTALL,null);
                            if(!ring.head.next.port.equals(msgs[1])) {
                                String[] msg = new String[3];
                                msg[0] = DynamoResources.DELETE;
                                msg[1] = msgs[1];
                                msg[2] = ring.head.next.port;
                                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msg);
                                out.writeObject(new String(DynamoResources.OK));
                                out.flush();
                            }
                        }
                    }
                    else if(msgs[0].equals(DynamoResources.FAILED))
                    {
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Failed message");
                        publishProgress(new String[]{msgs[0], msgs[1]});

                    }
                    else if(msgs[0].equals(DynamoResources.RECOVERY))
                    {
                        Log.d(DynamoResources.TAG,"Count "+count++ +" Recovery message by "+msgs[1]);
                        if(msgs.length > 2  )
                            publishProgress(new String[]{msgs[0], msgs[2], msgs[1]});
                    }

                    else if(msgs[0].equals(DynamoResources.CHECK))
                    {
                        Log.d(DynamoResources.TAG,"CHECK Message");
                        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.OK, msgs[1]});
                    }
                    else if(msgs[0].equals(DynamoResources.OK))
                    {
                        Log.d(DynamoResources.TAG,"OK Message");
                        queryBlock.clear();
                        queryBlock.put(DynamoResources.OK);
                    }
//                    else if(msgs[0].equals(DynamoResources.REPLQUERY))
//                    {
//
//                    }
                    read.close();
                    in.close();
                    out.close();
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
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Query Requested result count "+resultCursor.getCount()+" Columns "+resultCursor.getColumnCount());
                    String message = "";
                    if(resultCursor.getCount() > 0 && resultCursor.getColumnCount() == 3) {
                        resultCursor.moveToFirst();
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
                    else {
                        Log.d(DynamoResources.TAG,"Result Column count "+resultCursor.getColumnCount()+" less than 3. Dont know fuking why");
                        Log.d(DynamoResources.TAG,"Dumping "+resultCursor.getColumnNames());
                    }

                }
                else
                {
                    Cursor resultCursor = query(mUri, null,
                            values[1]+DynamoResources.separator+DynamoResources.SINGLE, null, null);
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Query Requested result count "+resultCursor.getCount()+"");
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
//                        failedVersions.put(m[0],Integer.parseInt(m[2]));
                        myInsert(cv,DynamoResources.REPLICATION);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                }
                failedCoorMap.put(values[2],dummy);
            }
            else if(values[0].equals(DynamoResources.FAILED))
            {
                Log.d(DynamoResources.TAG,"Count "+count++ +" Instruction to delete all");

                delete(mUri,DynamoResources.SELECTLOCAL,null);
//                insertBlock.clear();
//                try {
//                    insertBlock.put(values[1]);
//                } catch (InterruptedException e) {
//                    Log.e(DynamoResources.TAG,"Count "+count++ +" Interrupted in Publish progress");
//                    e.printStackTrace();
//                }
            }

//            else if()
        }

    }

    public static class ClientTask extends AsyncTask<String, String, Void> {

        @Override
        protected Void doInBackground(String... params) {

            String msgToSend = "";
            String portToSend = "";
            if (params[0].equals(DynamoResources.JOINING))
            {
                portToSend = params[2];
                Log.v(DynamoResources.TAG,"Inside ClientTask "+portToSend);
                String answers = SendReceiveMessage(portToSend,params[1]);
                if(answers != null)
                {
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Heartbeat of " + params[2]);
                    if(!failedCoorMap.containsKey(params[2]))
                        failedCoorMap.put(params[2],new Hashtable<String, String>());

                    try {
                        adjustNode(portToSend);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }

                    if(!answers.equals(DynamoResources.OK))
                    {
                        publishProgress(new String[]{DynamoResources.RECOVERY, answers, params[2]});
                    }
                }
            }

            else if (params[0].equals(DynamoResources.HEARTBEAT))
            {
                if(params.length == 3)
                    msgToSend = params[0] + DynamoResources.separator + myPort + DynamoResources.separator + params[2];
                else
                    msgToSend = params[0] + DynamoResources.separator + myPort;
                portToSend = params[1];
                sendMessage(portToSend, msgToSend);
                cancel(true);
            }

            else if (params[0].equals(DynamoResources.COORDINATION) || params[0].equals(DynamoResources.REPLICATION))
            {
                Log.d(DynamoResources.TAG, "Inside Coor or Repl Message");
                msgToSend = params[1];
                portToSend = params[2];
                if(SendReceiveMessage(portToSend,msgToSend) != null)
                {
                    Log.d(DynamoResources.TAG, "Received verification for key "+params[3]+" by "+portToSend);
                    if(insertMap.containsKey(params[3]))
                    {
                        Log.d(DynamoResources.TAG,"Key status in Map, Count = "+insertMap.get(params[3]));
                        if(insertMap.get(params[3]) + 1 == 2) {
                            Log.d(DynamoResources.TAG,"Unlocking the lock for key "+params[2]);
                            keyLockMap.put(params[3], 2);
                        }
                        else
                            insertMap.put(params[3],insertMap.get(params[3]) + 1);
                    }
                }
                cancel(true);
            }

            else if (params[0].equals(DynamoResources.QUERY)) {
                msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + myPort + DynamoResources.separator + params[3]
                    + DynamoResources.separator + params[4];
                portToSend = params[2];
                String answer = SendReceiveMessage(portToSend, msgToSend);

                if (params[4].equals(DynamoResources.SELECTLOCAL)) {
                    if(answer == null) answer = "";
                    queryAll.put(portToSend,answer);
                    if(queryAll.size() >= 4){
                        String result = "";
                        for(String key : queryAll.keySet())
                        {
                            result = result + queryAll.get(key);
                        }
                        try {
                            Log.d(DynamoResources.TAG,"Waiting for queue to be empty ");
                            while(!queue.isEmpty()){}
                            queue.put(result);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

                else if(answer != null && !"".equals(answer)) {
                    if(!params[4].equals(DynamoResources.SELECTALL)){
                        if (queryCount.get(params[1]) < 2) {
                            String[] keyVal = answer.split(DynamoResources.valSeparator)[0].split(" ");
                            if (queryKeyVersion.containsKey(params[1])) {
                                Log.d(DynamoResources.TAG, "Inserting to map when already has key " + keyVal[0] + " " + keyVal[1]);
                                int version = queryKeyVersion.get(params[1]);
                                if (version < Integer.parseInt(keyVal[2])) {
                                    queryKeyVal.put(keyVal[0], keyVal[1]);
                                    queryKeyVersion.put(keyVal[0], Integer.parseInt(keyVal[2]));
                                }
                            } else {
                                Log.d(DynamoResources.TAG, "Inserting to map when not has key " + keyVal[0]);
                                queryKeyVal.put(keyVal[0], keyVal[1]);
                                queryKeyVersion.put(keyVal[0], Integer.parseInt(keyVal[2]));
                            }

                            int qCount = queryCount.get(params[1]);
                            qCount++;
                            queryCount.put(params[1],qCount);
                            if (qCount == 2) {
                                Log.d(DynamoResources.TAG, "Total count 2 for key " + params[0]);
//                                keyLockMap.put(params[1], 2);
                                String message = params[1]+" "+queryKeyVal.get(params[1])+" "+queryKeyVersion.get(params[1])+DynamoResources.valSeparator;
                                Log.d(DynamoResources.TAG,"Generating Message "+message);
                                try {
                                    Log.d(DynamoResources.TAG,"Waiting for queue to be empty "+message);
                                    while(!queue.isEmpty()){}
                                    queue.put(message);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }
                cancel(true);
            }
            else if (params[0].equals(DynamoResources.FAILED)) {
//                msgToSend = params[0] + DynamoResources.separator + DynamoResources.SELECTLOCAL;
//                portToSend = myPort;
//                sendMessage(portToSend, msgToSend);
                messengerDb = dbHelper.getWritableDatabase();
                messengerDb.delete(DynamoResources.TABLE_NAME, null, null);
                cancel(true);
            }

            else if (params[0].equals(DynamoResources.QUERYREPLY)) {
                if(params.length == 4)
                    msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + params[2];
                else
                    msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + params[2] + DynamoResources.separator + params[4];
                portToSend = params[3];
                sendMessage(portToSend, msgToSend);
            } else if (params[0].equals(DynamoResources.DELETE)) {
                String[] senderList = params[2].split(DynamoResources.valSeparator);
                Log.v(DynamoResources.TAG,"Message to send on the delete request "+params[1]);
                for(String port : senderList)
                {
                    msgToSend = params[0] + DynamoResources.separator + params[1];
                    portToSend = port;
                    SendReceiveMessage(portToSend, msgToSend);
                }

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

                cancel(true);
            }

            else if (params[0].equals(DynamoResources.OK)) {
                msgToSend = DynamoResources.OK+DynamoResources.separator;
                portToSend = params[1];
                sendMessage(portToSend, msgToSend);
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            super.onProgressUpdate(values);

            if(values[0].equals(DynamoResources.RECOVERY))
            {
                Log.d(DynamoResources.TAG, "Starting Recovery");
                String[] msg = values[1].split(DynamoResources.separator);
                if(msg[0].equals(DynamoResources.REPLICATION)) {
                    Log.d(DynamoResources.TAG,"Recovery of Replicated messages");
                    Hashtable<String, String> dummy = failedCoorMap.get(values[2]);
                    String message = msg[1];
                    String[] keyVal = message.split(DynamoResources.valSeparator);
                    for (String kv : keyVal) {
                        String[] m = kv.split(" ");
                        ContentValues cv = new ContentValues();
                        cv.put(DynamoResources.KEY_COL, m[0]);
                        cv.put(DynamoResources.VAL_COL, m[1]);
                        cv.put(DynamoResources.VERSION, Integer.parseInt(m[2]));
                        try {
                            dummy.put(m[0], m[1]);
//                        failedVersions.put(m[0],Integer.parseInt(m[2]));
                            myInsert(cv, DynamoResources.REPLICATION);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                    }
                    failedCoorMap.put(values[2], dummy);
                }
                else
                {
                    Log.d(DynamoResources.TAG,"Recovery of My original messages");
                    String message = msg[1];
                    String[] keyVal = message.split(DynamoResources.valSeparator);
                    for (String kv : keyVal) {
                        String[] m = kv.split(" ");
                        ContentValues cv = new ContentValues();
                        cv.put(DynamoResources.KEY_COL, m[0]);
                        cv.put(DynamoResources.VAL_COL, m[1]);
                        cv.put(DynamoResources.VERSION, Integer.parseInt(m[2]));
                        try {
                            myKeysMap.put(m[0], m[1]);
//                        failedVersions.put(m[0],Integer.parseInt(m[2]));
                            myInsert(cv, DynamoResources.REPLICATION);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        }
                    }
                }
                cancel(true);
            }
        }


        private synchronized static String SendReceiveMessage(String portToSend, String msgToSend) {
            try {
                Log.d(DynamoResources.TAG,"Sending by Client to "+portToSend + " for message "+msgToSend);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(portToSend));
                socket.setSoTimeout(3000);
                OutputStream out = socket.getOutputStream();
                ObjectOutputStream writer = new ObjectOutputStream(out);

                ObjectInputStream in;
                writer.writeObject(msgToSend);
                writer.flush();
                in = new ObjectInputStream(socket.getInputStream());
                String msg = (String)in.readObject();
                Log.d(DynamoResources.TAG,"Returned message "+msg+" by "+portToSend);
//                writer.write(msgToSend);
                writer.close();
                out.close();
                in.close();
                socket.close();
                return msg;

            } catch (UnknownHostException e) {
                e.printStackTrace();
                Log.e(DynamoResources.TAG,"Count "+count++ +" ClientTask UnknownHostException");
            } catch (IOException e) {
                e.printStackTrace();
                Log.e(DynamoResources.TAG,"Count "+count++ +" ClientTask IOException for " + portToSend + " for message "+msgToSend);
                return null;
            } catch (Exception e) {
                e.printStackTrace();
                Log.e(DynamoResources.TAG,"Count "+count++ +" ClientTask Exception");
                return null;
            }
            return null;
        }

        private synchronized static void sendMessage(String portToSend, String msgToSend) {
            try {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(portToSend));
//                socket.setSoTimeout();
                OutputStream out = socket.getOutputStream();
//                OutputStreamWriter writer = new OutputStreamWriter(out);
                ObjectOutputStream writer = new ObjectOutputStream(out);
                ObjectInputStream in;
                writer.writeObject(msgToSend);
                writer.flush();
                in = new ObjectInputStream(socket.getInputStream());
                Log.d(DynamoResources.TAG,(String)in.readObject()+"-------------------------------------------------");
//                writer.write(msgToSend);

                writer.close();
                out.close();
                socket.close();
                in.close();

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

            String hash1 = DynamoResources.genHash(remotePorts.get("11108"));
            String hash2 = DynamoResources.genHash(remotePorts.get("11112"));
            String hash3 = DynamoResources.genHash(remotePorts.get("11116"));
            String hash4 = DynamoResources.genHash(remotePorts.get("11120"));
            String hash5 = DynamoResources.genHash(remotePorts.get("11124"));
            Node n1 = new Node("11108",hash1);
            Node n2 = new Node("11112",hash2);
            Node n3 = new Node("11116",hash3);
            Node n4 = new Node("11120",hash4);
            Node n5 = new Node("11124",hash5);
            n1.next = n3;
            n2.next = n1;
            n3.next = n4;
            n4.next = n5;
            n5.next = n2;
            n1.previous = n2;
            n2.previous = n5;
            n3.previous = n1;
            n4.previous = n3;
            n5.previous = n4;
            fixRing = new ChordLinkList(n1,n2,n3,n4,n5,myPort);

            String[] message = new String[3];
            message[0] = DynamoResources.JOINING;
            message[1] = DynamoResources.JOINING + DynamoResources.separator+myPort;

            for(String port: remotePorts.keySet())
            {
                if(!port.equals(myPort))
                {                    Log.v(DynamoResources.TAG,"Sending joining to "+port);
                    message[2] = port;
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.JOINING,
                            DynamoResources.JOINING + DynamoResources.separator+myPort, port});
                }
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private static String lookUpCoordinator(String key, String hashKey)
    {
        if(ring.size() == 5) {
            synchronized (ring) {
                Node current = ring.head;
                Node pre = current.previous;
//            Node next = current.next;
                String coordinator = "";
                Log.d(DynamoResources.TAG, "Count " + count++ + " Looking up where to store the " + key);

                while (true) {
                    if (pre.hashPort.compareTo(current.hashPort) > 0 && (hashKey.compareTo(pre.hashPort) > 0 || hashKey.compareTo(current.hashPort) < 0)) {
                        coordinator = current.port;
                        break;
                    } else if (current.hashPort.compareTo(hashKey) >= 0 && hashKey.compareTo(pre.hashPort) > 0) {
                        coordinator = current.port;
                        break;
                    } else {
                        pre = current;
                        current = current.next;
                    }

                    if (current == ring.head)
                        break;
                }

                return coordinator;
            }
        }
        else
        {
            synchronized (fixRing) {
                Node current = fixRing.head;
                Node pre = current.previous;
//            Node next = current.next;
                String coordinator = "";
                Log.d(DynamoResources.TAG, "Count " + count++ + " Looking up where to store the " + key);

                while (true) {
                    if (pre.hashPort.compareTo(current.hashPort) > 0 && (hashKey.compareTo(pre.hashPort) > 0 || hashKey.compareTo(current.hashPort) < 0)) {
                        coordinator = current.port;
                        break;
                    } else if (current.hashPort.compareTo(hashKey) >= 0 && hashKey.compareTo(pre.hashPort) > 0) {
                        coordinator = current.port;
                        break;
                    } else {
                        pre = current;
                        current = current.next;
                    }

                    if (current == fixRing.head)
                        break;
                }

                return coordinator;
            }
        }
    }

    private String recovery(String port, String type)
    {
        Log.d(DynamoResources.TAG,"Count "+count++ +" Generating Recovery message "+failedCoorMap.keySet().toString() +" port "+port);
        String result = "";
        if(type.equals(DynamoResources.REPLICATION)) {
            Log.d(DynamoResources.TAG,"Count "+count++ +" Inside");
            if (failedCoorMap.containsKey(port)) {
                Log.d(DynamoResources.TAG,"Count "+count++ +" It has some keys for " + port);
                Hashtable<String, String> dummy = failedCoorMap.get(port);
                for (Map.Entry<String, String> entry : dummy.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    Log.d(DynamoResources.TAG,"Count "+count++ +" Adding key " + key + "and value " + value);
                    result = result + key + " " + value + " " + failedVersions.get(key) + DynamoResources.valSeparator;
                }
//            failedCoorMap.put(port, new Hashtable<String, String>());
            }
//            Log.d(DynamoResources.TAG,"Count "+count++ +" It has some keys for " + port);
//            Hashtable<String, String> dummy = failedCoorMap.get(port);
//            for (Map.Entry<String, String> entry : dummy.entrySet()) {
//                String key = entry.getKey();
//                String value = entry.getValue();
//                Log.d(DynamoResources.TAG,"Count "+count++ +" Adding key " + key + "and value " + value);
//                result = result + key + " " + value + " " + failedVersions.get(key) + DynamoResources.valSeparator;
//            }
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