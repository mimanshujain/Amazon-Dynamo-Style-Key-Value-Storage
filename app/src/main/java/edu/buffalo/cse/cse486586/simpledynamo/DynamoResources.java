package edu.buffalo.cse.cse486586.simpledynamo;

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
}
