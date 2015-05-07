package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentResolver;
import android.content.Context;
import android.os.Bundle;
import android.app.Activity;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

import java.io.IOException;
import java.net.ServerSocket;
import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoProvider.*;

public class SimpleDynamoActivity extends Activity {

    static String myPort = "";
    static final int SERVER_PORT = 10000;
    static final String TAG = SimpleDynamoActivity.class.getSimpleName();

    @Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());


//        SimpleDynamoProvider obj1 =  new SimpleDynamoProvider();
//
//        SimpleDynamoProvider.startUp(myPort);
//        String[] message = new String[3];
//        message[0] = DynamoResources.JOINING;
//        message[1] = myPort;
////        SimpleDynamoProvider.ClientTask obj = obj1.new ClientTask();
////        obj.executeOnExecutor(SimpleDynamoProvider.myPool, message);
//        ClientTask obj = new ClientTask();
//        obj.executeOnExecutor(SimpleDynamoProvider.myPool, message);
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

}
