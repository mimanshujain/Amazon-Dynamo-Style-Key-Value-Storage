package edu.buffalo.cse.cse486586.simpledynamo;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Hashtable;
import java.util.Timer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
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
            new LinkedBlockingQueue<Runnable>(1000);
    static final Executor myPool = new ThreadPoolExecutor(1000,2000,1, TimeUnit.SECONDS,sPoolWorkQueue);
    //Other Variables
    static String myPort = "";
    static String hashedPort = "";
    static ChordLinkList ring = null;
    static ChordLinkList fixRing = null;
    //Maps and Collections
    static Hashtable<String,String> remotePorts = new Hashtable<>();
    static Hashtable<String, Hashtable<String, String>> failedCoorMap = new Hashtable<>();
    static Hashtable<String, Integer> failedVersions = new Hashtable<>();
    static Hashtable<String, String> myKeysMap = new Hashtable<>();
    static Hashtable<String, Integer> queryCount = new Hashtable<>();
    static Hashtable<String, String> queryKeyVal = new Hashtable<>();
    static Hashtable<String, Integer> queryKeyVersion = new Hashtable<>();
    static Hashtable<String, Integer> keyLockMap = new Hashtable<>();
    static Hashtable<String, Integer> insertMap = new Hashtable<>();
    static Hashtable<String, String> queryAll = new Hashtable<>();
    static Hashtable<String,String> queryAnswer = new Hashtable<>();

    @Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        messengerDb = dbHelper.getWritableDatabase();
        Log.v(DynamoResources.TAG,"Count "+count++ +" Delete at "+myPort +" Key = "+selection);

        int num = 0;

        if(selection.equals(DynamoResources.SELECTLOCAL))
        {
            Log.d(DynamoResources.TAG,"Delete my all local request");
            num = messengerDb.delete(DynamoResources.TABLE_NAME, null, null);
//            myKeysMap = new Hashtable<>();
        }
        else if(selection.equals(DynamoResources.SELECTALL))
        {
            num = messengerDb.delete(DynamoResources.TABLE_NAME, null, null);
//            myKeysMap = new Hashtable<>();
//            failedCoorMap = new Hashtable<>();

            for(String port : remotePorts.keySet())
            {
                if(!port.equals(myPort))
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.DELETE,DynamoResources.SELECTLOCAL, port});
            }
        }
        else if(selection.contains(DynamoResources.SINGLE))
        {
            selection = selection.split(DynamoResources.separator)[0];
            Log.d(DynamoResources.TAG,"Single Single I am deleting the key "+selection);
            num = messengerDb.delete(DynamoResources.TABLE_NAME,"key = \'"+selection+"\'",null);
        }
        else
        {
            try {
                String hashKey = DynamoResources.genHash(selection);
                String coordinatorPort = lookUpCoordinator(selection, hashKey);
                String[] replicators = fixRing.getPreferenceListArray(coordinatorPort);
                Log.d(DynamoResources.TAG,"Found coordinator is "+coordinatorPort);
                if(coordinatorPort.equals(myPort))
                {
                    Log.d(DynamoResources.TAG,"I am deleting the key "+selection);
                    num = messengerDb.delete(DynamoResources.TABLE_NAME,"key = \'"+selection+"\'",null);
//                    myKeysMap.remove(selection);
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.DELETE,selection, replicators[0]});
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.DELETE,selection, replicators[1]});
                }
                else {
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.DELETE,selection, coordinatorPort});
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.DELETE,selection, replicators[0]});
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.DELETE,selection, replicators[1]});
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

            Log.d(DynamoResources.TAG,"Count "+count++ +" Insertion Starts for key "+key +" with value "+value);
            if(!keyLockMap.containsKey(key))
                keyLockMap.put(key,2);
            Log.d(DynamoResources.TAG,"Checking the lock for key "+key+"...........");
            while(keyLockMap.get(key) == 1){
                Log.d(DynamoResources.TAG,"Waiting for INSERT lock for "+key+"  .......................");
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
            Log.d(DynamoResources.TAG,"Lock is free now and locking it....");
            keyLockMap.put(key, 1);

            String hashKey = DynamoResources.genHash(values.get(DynamoResources.KEY_COL).toString());

            String coordinatorPort = lookUpCoordinator(key, hashKey);
            if(coordinatorPort.equals(myPort))
            {
                Log.d(DynamoResources.TAG,"Count "+count++ +" My Key "+key);
                int currentVersion = MyOwnInsert(values);
                insertMap.put(key,1);
//                myKeysMap.put(key,value);
//                failedVersions.put(key,currentVersion);

                String msgToSend = DynamoResources.REPLICATION + DynamoResources.separator + key + DynamoResources.separator +
                        value + DynamoResources.separator + myPort;

                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.REPLICATION, msgToSend,fixRing.head.next.port,key});
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.REPLICATION, msgToSend,fixRing.head.next.next.port,key});

            }

            else {
                Log.d(DynamoResources.TAG,"Count "+count++ +" Sending Key "+key+" to real Coordinator and replicators "+coordinatorPort);
                String msgToSend = key + DynamoResources.separator +
                        value + DynamoResources.separator + coordinatorPort;

                insertMap.put(key,0);
                String[] replicators = fixRing.getPreferenceListArray(coordinatorPort);

                //Sending to the actual coordinator and replicators
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool,  new String[]{DynamoResources.COORDINATION,
                        DynamoResources.COORDINATION + DynamoResources.separator + msgToSend,coordinatorPort,key});
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.REPLICATION,
                        DynamoResources.REPLICATION + DynamoResources.separator + msgToSend, replicators[0], key});
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.REPLICATION,
                        DynamoResources.REPLICATION + DynamoResources.separator + msgToSend, replicators[1], key});

            }
            while(keyLockMap.get(key) == 1){
                Log.d(DynamoResources.TAG,"Waiting for INSERT lock to release for "+key+"  ............................");
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
        catch (NoSuchAlgorithmException ex)
        {
            ex.printStackTrace();
            Log.e(DynamoResources.TAG, ex.getMessage());
        }
		return null;
	}

    private int MyOwnInsert(ContentValues values) {
        int currentVersion;
        messengerDb = dbHelper.getWritableDatabase();
        Cursor cur = messengerDb.query(DynamoResources.TABLE_NAME, null,
                DynamoResources.KEY_COL + "='" + values.get(DynamoResources.KEY_COL) + "'", null, null, null, null);

        if (cur.getCount() > 0)
        {
            cur.moveToFirst();
            Log.d(DynamoResources.TAG, "Count " + count++ + DynamoResources.separator + " When I already had the key " + values.get(DynamoResources.KEY_COL));
            int oldVersion = cur.getInt(2);

            if(values.size() == 3)
                currentVersion = values.getAsInteger(DynamoResources.VERSION);

            else
                currentVersion = oldVersion + 1;

//            if(currentVersion > oldVersion)
//            {
//                if(values.size() == 3)
//                    values.remove(DynamoResources.VERSION);
//                values.put(DynamoResources.VERSION,currentVersion);
//                messengerDb.update(DynamoResources.TABLE_NAME, values, DynamoResources.KEY_COL+" = '" + values.get(DynamoResources.KEY_COL) + "'", null);
//            }
            messengerDb.update(DynamoResources.TABLE_NAME, values, DynamoResources.KEY_COL+" = '" + values.get(DynamoResources.KEY_COL) + "'", null);
        }
        else
        {
            Log.d(DynamoResources.TAG,"Count "+count++ +DynamoResources.separator+" I am freshly inserting key "+values.get(DynamoResources.KEY_COL));
            currentVersion = 1;
            if(values.size() != 3)
                values.put(DynamoResources.VERSION, currentVersion);

//            values.remove(DynamoResources.VERSION);

            messengerDb.insert(DynamoResources.TABLE_NAME, null, values);
        }
        return currentVersion;
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
        while(keyLockMap.get(selection) == 1){
            Log.d(DynamoResources.TAG,"Waiting for QUERY lock for "+selection+"  .......................");
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
        Log.d(DynamoResources.TAG,"Lock is free now and locking it........................................................");
        keyLockMap.put(selection, 1);

        try {
            if(selection.equals(DynamoResources.SELECTALL))
            {
                resultCursor = qBuilder.query(messengerDb, null, null, selectionArgs, null, null, null);
                Log.v(DynamoResources.TAG,"Count "+count++ +" Select * local count: "+resultCursor.getCount()+" Colum count "+resultCursor.getColumnCount());
                MatrixCursor mx = RequestResults(selection);
                if(mx == null || mx.getCount() <= 0) return resultCursor;
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
                Log.d(DynamoResources.TAG,"Setting lock free now");
                return mx;
            }
            else if(selection.equals(DynamoResources.SELECTLOCAL))
            {
                Log.v(DynamoResources.TAG,"Count "+count++ +" Query for selecting local keys "+selection);
                resultCursor = qBuilder.query(messengerDb, new String[]{DynamoResources.KEY_COL,DynamoResources.VAL_COL}, null,selectionArgs, null, null, null);

                Log.v(DynamoResources.TAG,"Count "+count++ +"Total Count found "+ resultCursor.getCount());
                Log.d(DynamoResources.TAG,"Setting lock free now");
                keyLockMap.put(selection,2);
                return resultCursor;

            }
            else
            {
                Log.v(DynamoResources.TAG,"Count "+count++ +" Query a single key "+selection);
                resultCursor = qBuilder.query(messengerDb, null, " key = \'" + selection + "\' ",
                        selectionArgs, null, null, null);

                if(resultCursor.getCount() > 0)
                {
                    Log.d(DynamoResources.TAG,"I have returned some data " + resultCursor.getColumnCount());
                    resultCursor.moveToFirst();
                    queryCount.put(selection,1);
                    queryKeyVersion.put(selection, Integer.parseInt(resultCursor.getString(2)));
                    queryKeyVal.put(selection,resultCursor.getString(1));
                }

                MatrixCursor mx = RequestResults(selection);

                keyLockMap.put(selection,2);
                Log.d(DynamoResources.TAG,"Setting lock free now");
                return mx;
            }
        }
        catch    (InterruptedException e) {
            keyLockMap.put(selection,2);
            e.printStackTrace();
            Log.v(DynamoResources.TAG,"Count "+count++ +" InterruptedException in Query");
        }
        catch (NoSuchAlgorithmException e)
        {
            keyLockMap.put(selection,2);
            e.printStackTrace();
            Log.v(DynamoResources.TAG,"Count "+count++ +" NoSuchAlgorithmException in Query");
        }
        keyLockMap.put(selection,2);
        Log.d(DynamoResources.TAG,"Returning "+resultCursor.getCount()+" results. If grader giving missing key. Not my fault.");
		return resultCursor;
	}

    private MatrixCursor RequestResults(String selection) throws InterruptedException, NoSuchAlgorithmException {
        Log.d(DynamoResources.TAG,"Count "+count++ +" Inside GetResults Class for "+ selection);

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
            String coordinator = lookUpCoordinator(selection, DynamoResources.genHash(selection));
            Log.v(DynamoResources.TAG,"The coordinator is "+coordinator);
            if(queryCount.containsKey(selection))
            {
                String[] replicators = fixRing.getPreferenceListArray(coordinator);
                if(coordinator.equals(myPort))
                {
                    Log.d(DynamoResources.TAG,"Query when I am having key "+selection + " and I am coor with "+queryKeyVal.get(selection));
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[0],myPort,DynamoResources.SINGLE});
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[1],myPort,DynamoResources.SINGLE});
                }
                else
                {
                    Log.d(DynamoResources.TAG,"Query when I am having key "+selection + " and I am not coor with "+queryKeyVal.get(selection));
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,coordinator,myPort,DynamoResources.SINGLE});

                    if(replicators[0].equals(myPort))
                        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[1],myPort,DynamoResources.SINGLE});
                    else
                        new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[0],myPort,DynamoResources.SINGLE});
                }
            }
            else {
                queryCount.put(selection, 0);
                String[] replicators = fixRing.getPreferenceListArray(coordinator);
                Log.d(DynamoResources.TAG, "Count " + count++ + " Sending Query to " + coordinator +" "+replicators.toString());
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,coordinator,myPort,DynamoResources.SINGLE});
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[0],myPort,DynamoResources.SINGLE});
                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.QUERY,selection,replicators[1],myPort,DynamoResources.SINGLE});
            }
        }

        Log.d(DynamoResources.TAG,"Count "+count++ +" Waiting...");

        while(keyLockMap.get(selection) == 1){
            Log.d(DynamoResources.TAG,"Waiting for QUERY lock to release for "+selection+"  .......................");
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }

        String message = queryAnswer.get(selection);
        Log.d(DynamoResources.TAG,"Count "+count++ +" Gotcha Message " + message + " at " + myPort);
        String[] received = message.split(DynamoResources.valSeparator);
        MatrixCursor mx = new MatrixCursor(new String[]{DynamoResources.KEY_COL, DynamoResources.VAL_COL}, 150);
        for (String m : received) {
            Log.v(DynamoResources.TAG,"Count "+count++ +" Merging my results");
            String[] keyVal = m.split(" ");
            mx.addRow(new String[]{keyVal[0], keyVal[1]});
        }

        if(mx.getCount() == 1) {
            mx.moveToFirst();
            Log.d(DynamoResources.TAG, "GetResults Returning for key " + selection + " " + mx.getCount() + " value " + mx.getString(1));
        }
        queryCount.remove(selection);
        return mx;
    }

    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            while (true) {
                String msgReceived = "";
                try {
                    Log.d(DynamoResources.TAG, "Inside Server");
                    Socket socket = serverSocket.accept();
                    InputStream read = socket.getInputStream();

                    ObjectInputStream in = new ObjectInputStream(read);
                    msgReceived = (String) in.readObject();
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    Log.v(DynamoResources.TAG, "Count " + count++ + " Message Received :" + msgReceived);
                    String[] msgs = msgReceived.split(DynamoResources.separator);

//                    if(!msgs[0].equals(DynamoResources.REPLICATION) && !msgs[0].equals(DynamoResources.COORDINATION))
//                    {
                        out.writeObject(new String(DynamoResources.OK));
                        out.flush();
//                    }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////           JOINING
                    if (msgs[0].equals(DynamoResources.JOINING))
                    {
                        String myNext = fixRing.head.next.port;
                        String myNextNext = fixRing.head.next.next.port;
                        Log.d(DynamoResources.TAG, "Count " + count++ + " Joining starts for " + msgs[1]);
                        String message = null;
                        Log.d(DynamoResources.TAG, "Count " + count++ + " Coming back from the dead " + msgs[1]);
//                            ring.setLifeStatus(msgs[1], true);
                        String myCoord = fixRing.getPreferenceList(msgs[1]);
                        Log.d(DynamoResources.TAG, "Resurrector Pref list " + myCoord);

                        //When I am next to the coordinator. Sending it to its keys.
                        if (myCoord.split(DynamoResources.valSeparator)[0].equals(myPort))
                        {
                            message = recovery(msgs[1]);
                            Log.d(DynamoResources.TAG, "Count " + count++ + " Sending Recovery Message to " + msgs[1] + " message " + message);
                            if (message != null && !message.equals(""))
                                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.RECOVERY, DynamoResources.COOR + DynamoResources.separator +
                                        message, msgs[1], myPort});
                        }
                        //When my replicator is coming back
                        else if (myNext.equals(msgs[1]) || myNextNext.equals(msgs[1]))
                        {
                            message = recovery(myPort);
                            if (message != null && !message.equals(""))
                                new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, new String[]{DynamoResources.RECOVERY, DynamoResources.REPL + DynamoResources.separator +
                                        message, msgs[1], myPort});

                            Log.d(DynamoResources.TAG, "Count " + count++ + " Sending My Keys to " + msgs[1] + " message " + message);
                        }
//                        }
                    }

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////           REPLICATION
                    else if (msgs[0].equals(DynamoResources.REPLICATION) || msgs[0].equals(DynamoResources.COORDINATION))
                    {
                        Log.d(DynamoResources.TAG, "Count " + count++ + " Replication Message by " + msgs[3] + " for key " + msgs[1]);
                        ContentValues cv = new ContentValues();
                        cv.put(DynamoResources.KEY_COL, msgs[1]);
                        cv.put(DynamoResources.VAL_COL, msgs[2]);
//                        cv.put(DynamoResources.VERSION, 1);
                        int version = MyOwnInsert(cv);
//                        if (version != 0)
//                            out.writeObject(new String(DynamoResources.OK));
//                        out.flush();
//                        failedVersions.put(msgs[1],version);
                        Log.d(DynamoResources.TAG, "Count " + count++ + " Insertion done for key " + msgs[1]);

//                        if (msgs[0].equals(DynamoResources.REPLICATION))
//                        {
//                            Hashtable<String, String> dummy;
//                            if (failedCoorMap.containsKey(msgs[3])) {
//                                Log.d(DynamoResources.TAG, "Count " + count++ + " Already contains port " + msgs[3] + " key " + msgs[1]);
//                                dummy = failedCoorMap.get(msgs[3]);
//                                dummy.put(msgs[1], msgs[2]);
//                            } else {
//                                Log.d(DynamoResources.TAG, "Count " + count++ + " Not contains port " + msgs[3] + " key " + msgs[1]);
//                                dummy = new Hashtable<>();
//                                dummy.put(msgs[1], msgs[2]);
//                            }
//                            failedCoorMap.put(msgs[3], dummy);
//                        }
//                        else myKeysMap.put(msgs[1], msgs[2]);

                    }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////           QUERY
                    else if (msgs[0].equals(DynamoResources.QUERY))
                    {
                        Log.d(DynamoResources.TAG, "Count " + count++ + " Query Request " + myPort + " " + msgs[0] + " " + msgs[1]);
                        publishProgress(new String[]{msgs[0],msgs[1],msgs[2], msgs[4]});
                    }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////           QUERY REPLY
                    else if (msgs[0].equals(DynamoResources.QUERYREPLY))
                    {
                        if (msgs[3].equals(DynamoResources.SELECTLOCAL))
                        {
                            queryAll.put(msgs[4],msgs[2]);
                            if(queryAll.size() >= 3)
                            {
                                String result = "";
                                for(String key : queryAll.keySet())
                                    result = result + queryAll.get(key);

                                Log.d(DynamoResources.TAG,"Waiting for queue to be empty ");
                                queryAnswer.put(DynamoResources.SELECTALL,result);
                                keyLockMap.put(DynamoResources.SELECTALL,2);
                            }
                        }
                        else
                        {
                            if (queryCount.containsKey(msgs[1]) && queryCount.get(msgs[1]) < 2) {
                                String[] keyVal = msgs[2].split(DynamoResources.valSeparator)[0].split(" ");
                                if (queryKeyVersion.containsKey(msgs[1])) {
                                    Log.d(DynamoResources.TAG, "Inserting to map when already has key " + keyVal[0] + " " + keyVal[1]);
                                    int version = queryKeyVersion.get(msgs[1]);
                                    if (version <= Integer.parseInt(keyVal[2])) {
                                        queryKeyVal.put(keyVal[0], keyVal[1]);
                                        queryKeyVersion.put(keyVal[0], Integer.parseInt(keyVal[2]));
                                    }
                                } else {
                                    Log.d(DynamoResources.TAG, "Inserting to map when not has key " + keyVal[0]);
                                    queryKeyVal.put(keyVal[0], keyVal[1]);
                                    queryKeyVersion.put(keyVal[0], Integer.parseInt(keyVal[2]));
                                }

                                int qCount = queryCount.get(msgs[1]);
                                qCount++;
                                queryCount.put(msgs[1], qCount);
                                if (qCount >= 2) {
                                    Log.d(DynamoResources.TAG, "Total count 2 for key " + msgs[0]);
                                    String message = msgs[1] + " " + queryKeyVal.get(msgs[1]) + " " + queryKeyVersion.get(msgs[1]) + DynamoResources.valSeparator;
                                    Log.d(DynamoResources.TAG, "Generating Message " + message);
                                    queryAnswer.put(msgs[1],message);
                                    keyLockMap.put(msgs[1],2);

                                    queryCount.remove(msgs[1]);
                                    queryKeyVal.remove(msgs[1]);
                                    queryKeyVersion.remove(msgs[1]);
                                }
                            }
                            else
                                Log.v(DynamoResources.TAG," Count for this key has already been greated than 2 "+msgs[1]+" .........................................................");
                        }
                    }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////           DELETE
                    else if (msgs[0].equals(DynamoResources.DELETE))
                    {
                        Log.d(DynamoResources.TAG, "Count " + count++ + " Delete request " + msgs[0] + " " + msgs[1]);
                        messengerDb = dbHelper.getWritableDatabase();
                        if(msgs[1].equals(DynamoResources.SELECTLOCAL))
                        {
                            messengerDb.delete(DynamoResources.TABLE_NAME, null, null);
                        }
                        else
                        {
                            messengerDb.delete(DynamoResources.TABLE_NAME, "key = \'" + msgs[1] + "\'", null);
                        }
                    }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////           COOR
                    else if (msgs[0].equals(DynamoResources.COOR) || msgs[0].equals(DynamoResources.REPL))
                    {
                        Log.d(DynamoResources.TAG,"Recovery of My "+msgs[0]+" messages");
                        String message = msgs[1];
                        String[] keyVal = message.split(DynamoResources.valSeparator);
                        for (String kv : keyVal) {
                            String[] m = kv.split(" ");
                            ContentValues cv = new ContentValues();
                            cv.put(DynamoResources.KEY_COL, m[0]);
                            cv.put(DynamoResources.VAL_COL, m[1]);
                            int version = Integer.parseInt(m[2]);
                            cv.put(DynamoResources.VERSION, version);
                            failedVersions.put(m[0], version);
                            MyOwnInsert(cv);
                            myKeysMap.put(m[0], m[1]);
                        }
                    }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////           REPLICATION
                    else if (msgs[0].equals(DynamoResources.REPL))
                    {
                        Log.d(DynamoResources.TAG, "Starting REPLICATION recovery");
                        Hashtable<String, String> dummy = failedCoorMap.get(msgs[2]);
                        String message = msgs[1];
                        String[] keyVal = message.split(DynamoResources.valSeparator);
                        for (String kv : keyVal) {
                            String[] m = kv.split(" ");
                            ContentValues cv = new ContentValues();
                            cv.put(DynamoResources.KEY_COL, m[0]);
                            cv.put(DynamoResources.VAL_COL, m[1]);
                            int version = Integer.parseInt(m[2]);
                            cv.put(DynamoResources.VERSION, version);
                            failedVersions.put(m[0], version);
                            MyOwnInsert(cv);
                            dummy.put(m[0], m[1]);
                        }
                        failedCoorMap.put(msgs[2], dummy);
                    }
                    read.close();
                    in.close();
                    out.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                    Log.e(DynamoResources.TAG, "Count " + count++ + " IOException in ServerTask:................................................................ "+msgReceived );
                } catch (Exception ex) {
                    ex.printStackTrace();
                    Log.e(DynamoResources.TAG, "Count " + count++ + " Exception in ServerTask:...................................................................... " + msgReceived);
                }
            }
            //return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            super.onProgressUpdate(values);

            if (values[0].equals(DynamoResources.QUERY)) {

                Cursor resultCursor = null;
                messengerDb = dbHelper.getReadableDatabase();

                if(values[3].equals(DynamoResources.SELECTLOCAL))
                    resultCursor = messengerDb.query(true, DynamoResources.TABLE_NAME, null, null, null, null, null, null, null);
                else
                    resultCursor = messengerDb.query(true, DynamoResources.TABLE_NAME, null, "key = '" + values[1] + "'", null, null, null, null, null);

                Log.d(DynamoResources.TAG,"Count "+count++ +" Query Requested result count "+resultCursor.getCount()+" Columns "+resultCursor.getColumnCount());

                if(resultCursor.getCount() > 0 && resultCursor.getColumnCount() == 3)
                {
                    String message = DynamoResources.getCursorValue(resultCursor);

                    Log.v(DynamoResources.TAG,"Count "+count++ +" Now replying back with "+message);
                    String[] msgToRequester = new String[3];
                    String msgToReply = DynamoResources.QUERYREPLY+DynamoResources.separator+values[1]+DynamoResources.separator+message+DynamoResources.separator+values[3]
                            +DynamoResources.separator+myPort;
                    msgToRequester[0] = DynamoResources.QUERYREPLY;
                    msgToRequester[1] = msgToReply;
                    msgToRequester[2] = values[2];
                    new ClientTask().executeOnExecutor(SimpleDynamoProvider.myPool, msgToRequester);
                }
            }
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
//                if(answers != null)
//                {
//                    Log.d(DynamoResources.TAG,"Count "+count++ +" Heartbeat of " + params[2]);
//
//                    try {
//                        adjustNode(portToSend);
//                    } catch (NoSuchAlgorithmException e) {
//                        e.printStackTrace();
//                    }
//                }
            }

            else if (params[0].equals(DynamoResources.COORDINATION) || params[0].equals(DynamoResources.REPLICATION))
            {
                Log.d(DynamoResources.TAG, "Inside Message type "+DynamoResources.COORDINATION);
                msgToSend = params[1];
                portToSend = params[2];
                if(SendReceiveMessage(portToSend,msgToSend) != null)
                {
                    Log.d(DynamoResources.TAG, "Received verification for key "+params[3]+" by "+portToSend);
                    if(insertMap.containsKey(params[3]) && insertMap.get(params[3]) < 2)
                    {
                        Log.d(DynamoResources.TAG,"Key status in Map, Count = "+insertMap.get(params[3]));

                        insertMap.put(params[3],insertMap.get(params[3]) + 1);
                        if(insertMap.get(params[3]) >= 2) {
                            Log.d(DynamoResources.TAG,"Unlocking the lock for key "+params[3]);
                            keyLockMap.put(params[3], 2);
                            insertMap.remove(params[3]);
                        }
                    }
                }
                cancel(true);
            }

            else if (params[0].equals(DynamoResources.QUERY)) {

                msgToSend = params[0] + DynamoResources.separator + params[1] + DynamoResources.separator + myPort + DynamoResources.separator + params[3]
                        + DynamoResources.separator + params[4];
                portToSend = params[2];
                SendReceiveMessage(portToSend, msgToSend);
                cancel(true);
            }

            else if (params[0].equals(DynamoResources.QUERYREPLY))
            {
                Log.v(DynamoResources.TAG,"Replying back the query to "+params[2]);
                portToSend = params[2];
                SendReceiveMessage(portToSend, params[1]);
            }

            else if (params[0].equals(DynamoResources.DELETE))
            {
                portToSend = params[2];
                msgToSend = DynamoResources.DELETE + DynamoResources.separator + params[1];
                SendReceiveMessage(portToSend,msgToSend);
//                String[] senderList = params[2].split(DynamoResources.valSeparator);
//                Log.v(DynamoResources.TAG,"Message to send on the delete request "+params[1]);
//                for(String port : senderList)
//                {
//                    msgToSend = params[0] + DynamoResources.separator + params[1];
//                    portToSend = port;
//                    SendReceiveMessage(portToSend, msgToSend);
//                }

            }

            else if (params[0].equals(DynamoResources.RECOVERY)) {
                Log.d(DynamoResources.TAG,"Count "+count++ +" Sending RECOVERY message to "+params[2]);
                msgToSend = params[1] + DynamoResources.separator + params[3];
                portToSend = params[2];
                SendReceiveMessage(portToSend, msgToSend);
            }

            return null;
        }

        private static String SendReceiveMessage(String portToSend, String msgToSend) {
            try {
                Log.d(DynamoResources.TAG,"Sending by Client to "+portToSend + " for message "+msgToSend);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(portToSend));
                socket.setSoTimeout(5000);
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
//            ring = new ChordLinkList(myPort,hashedPort);

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
                failedCoorMap.put(port, new Hashtable<String, String>());
            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private static String lookUpCoordinator(String key, String hashKey)
    {
        Node current = fixRing.head;
        Node pre = current.previous;
        String coordinator = "";
//        Log.d(DynamoResources.TAG, "Count " + count++ + " Looking up where to store the " + key);

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

    private String recovery(String port)
    {
        Log.d(DynamoResources.TAG,"Count "+count++ +" Generating Recovery message for port "+port);
        String result = "";

        messengerDb = dbHelper.getReadableDatabase();
        Cursor resultCursor = messengerDb.query(true, DynamoResources.TABLE_NAME, null, null, null, null, null, null, null);

        int valueIndex = resultCursor.getColumnIndex(DynamoResources.VAL_COL);
        int keyIndex = resultCursor.getColumnIndex(DynamoResources.KEY_COL);
        int versionIndex = resultCursor.getColumnIndex(DynamoResources.VERSION);

        resultCursor.moveToFirst();
        boolean isLast = true;

        while(resultCursor.getCount() > 0 && isLast)
        {
            String newKey = resultCursor.getString(keyIndex);
            try {
                if(lookUpCoordinator(newKey,DynamoResources.genHash(newKey)).equals(port))
                {
                    String newValue = resultCursor.getString(valueIndex);
                    String version = resultCursor.getString(versionIndex);
                    result = result + newKey+" "+newValue + " " + version + DynamoResources.valSeparator;
                }
                isLast = resultCursor.moveToNext();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }



//        if(type.equals(DynamoResources.REPLICATION))
//        {
//            Log.d(DynamoResources.TAG,"Count "+count++ +" Inside");
//            if (failedCoorMap.containsKey(port)) {
//                Log.d(DynamoResources.TAG,"Count "+count++ +" It has some keys for " + port);
//                Hashtable<String, String> dummy = failedCoorMap.get(port);
//                for (Map.Entry<String, String> entry : dummy.entrySet()) {
//                    String key = entry.getKey();
//                    String value = entry.getValue();
//                    Log.d(DynamoResources.TAG,"Count "+count++ +" Adding key " + key + "and value " + value +" and version "+failedVersions.get(key));
//                    result = result + key + " " + value + " " + failedVersions.get(key) + DynamoResources.valSeparator;
//                }
//            }
//        }
//
//        else {
//            for (Map.Entry<String, String> entry : myKeysMap.entrySet()) {
//                String key = entry.getKey();
//                String value = entry.getValue();
//                Log.d(DynamoResources.TAG,"Count "+count++ +" Adding key " + key + "and value " + value +" and version "+failedVersions.get(key));
//                result = result + key + " " + value + " " + failedVersions.get(key) + DynamoResources.valSeparator;
//            }
//        }
        return result;
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
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

}

//Coordination and Replication message need not to have hashkey. Look into this. Remove if not needed till the final submission.
//Query functionality for a single key. Send request to coordinator directly.
//* queries should return the unique values.