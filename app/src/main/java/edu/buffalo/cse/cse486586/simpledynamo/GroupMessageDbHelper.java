package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by sherlock on 4/24/15.
 */
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