package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Build;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {
    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    static final int SERVER_PORT = 10000;
    static final String DELIMETER = "##";
    static final String NODE_DEL = "//";
    static final String NODE_JOIN_REQUEST = "NodeJoin";
    static final String UPDATE = "UPDATE";
    static final String HEAD_NODE_PORT = "11108" ;
    static final String INSERT_DATA = "INSERT_DATA";
    static final String QUERY_DATA = "QUERY_DATA";
    static final String QUERY_ALL_DATA = "QUERY_ALL_DATA";
    static final String DELETE_DATA = "DELETE_DATA";
    static final String DELETE_ALL_DATA = "DELETE_ALL_DATA";
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    Node currentNode = null;
    String myPort ;
    TreeMap<String,String> nodeInfo = new TreeMap<String, String>();
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        if(selection.equals("*")){
            if(currentNode.successor == null && currentNode.predecessor == null){
                Log.e(TAG,"Single Node up case");
                deleteGlobalList();
            }
            String sucPort = currentNode.successor.port;
            String delAllMessage = DELETE_ALL_DATA + DELIMETER + "@" + DELIMETER + myPort;
            try {
                new Task().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE_ALL_DATA, delAllMessage, sucPort).get();
                Log.e(TAG, "MAIN DELETE : delete All call");
            } catch (InterruptedException e) {
                Log.e(TAG, "InterruptedException in query Task");
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
                Log.e(TAG, "ExecutionException in query Task");
            }

        }
        else
            if(selection.equals("@")){
            //Delete Local files
                List<String> allFiles = getAllFiles();
                for(String file : allFiles){
                    getContext().deleteFile(file);
                }
            }
             else{
                if(currentNode.successor == null && currentNode.predecessor == null){
                    Log.e(TAG,"Single Node up case");
                    if(getContext().deleteFile(selection)){
                        Log.e(TAG, "Deleted the file Name : " + selection);
                    }
                    else{
                        Log.e(TAG, "File not deleted : " + selection);
                    }
                }
                // handle siingle file deletion case

                String keyHash = null;
                try {
                    keyHash = genHash(selection);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }

                    Log.e(TAG, "Query: KeyHash ::" + keyHash +  "  for key: " +  selection);
                    if(currentNode.id.compareTo(currentNode.predecessor.id) < 0 && keyHash.compareTo(currentNode.id) > 0 &&
                            keyHash.compareTo(currentNode.predecessor.id)>0){
                        Log.e(TAG, "KeyHash is greater than largest Key so Data will be [here]");
                        getContext().deleteFile(selection);
                    }
                    else if(currentNode.id.compareTo(currentNode.predecessor.id) < 0 && keyHash.compareTo(currentNode.id) < 0){
                        Log.e(TAG, "KeyHash is less than smallest Key so Data will be [here]");
                        getContext().deleteFile(selection);
                    }
                    else if(keyHash.compareTo(currentNode.id) < 0 && keyHash.compareTo(currentNode.predecessor.id) > 0){
                        Log.e(TAG, "KeyHash is less than current Node id  and Greater than previous so Data will be here");
                        getContext().deleteFile(selection);
                    }
                    else {
                        Log.e(TAG, "MAIN DELETE : DELETE OTHER_NODE : KeyHash: " + keyHash + " is greater than current Node id " + currentNode.id);
                        // send the data to next node.
                        String sucPort = currentNode.successor.port;
                        String deleteMessage =  DELETE_DATA + DELIMETER + selection + DELIMETER  + myPort;

                        try {
                            new Task().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, DELETE_DATA, deleteMessage, sucPort).get();
                            Log.e(TAG, "MAIN DELETE : delete call");
                        } catch (InterruptedException e) {
                            Log.e(TAG, "InterruptedException in query Task");
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                            Log.e(TAG, "ExecutionException in query Task");
                        }

                    }


              }
        return 0;
    }

    private void deleteGlobalList() {
        List<String> allFiles = getAllFiles();
        for(String file : allFiles){
            getContext().deleteFile(file);
        }
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        Log.e(TAG, "MAIN INSERT: inside insert method");
        // We need to insert on proper place in ring
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        Log.e(TAG, "MAIN INSERT: Called for key:value = " + key + "::" + value);
        if(currentNode.successor == null && currentNode.predecessor == null){
            Log.e(TAG,"Single Node up case");
            storeContent(key,value);
            return uri;
        }
        try {
            String keyHash = genHash(key);
            Log.e(TAG, "INSERT: KeyHash ::" + keyHash +  "  for key: " +  key);
            if(currentNode.id.compareTo(currentNode.predecessor.id) < 0 && keyHash.compareTo(currentNode.id) > 0 &&
                    keyHash.compareTo(currentNode.predecessor.id)>0){
                Log.e(TAG, "KeyHash is greater than largest Key so insert in Lowest Key Node [here]");
                storeContent(key,value);
            }
            else if(currentNode.id.compareTo(currentNode.predecessor.id) < 0 && keyHash.compareTo(currentNode.id) < 0){
                Log.e(TAG, "KeyHash is less than smallest Key so insert in Lowest Key Node [here]");
                storeContent(key,value);
            }
            else if(keyHash.compareTo(currentNode.id) < 0 && keyHash.compareTo(currentNode.predecessor.id) > 0){
                Log.e(TAG, "KeyHash is less than current Node id  and Greater than previous");
                storeContent(key, value);
            }
            else /*if(keyHash.compareTo(currentNode.id) > 0)*/{
                Log.e(TAG, "KeyHash: " + keyHash + " is greater than current Node id " + currentNode.id);
                // send the data to next node.
                String sucPort = currentNode.successor.port;
                String dataMessage = INSERT_DATA + DELIMETER + key + DELIMETER + value + DELIMETER + sucPort;

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, INSERT_DATA, myPort, dataMessage, sucPort);

            }
            /*else{
                Log.e(TAG,"InsertMethod: No place found for this Message Key: " + key
                        + "  Value: " + value);
            }*/
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException keyHash Insert");
        }


        return uri;

    }

    public void storeContent(String key, String value){
        Log.e(TAG, "Main: Inside storeContent method");
        String filename = key;
        String string = value + "\n";
        FileOutputStream outputStream;
        Log.v("fileName: ", filename + " file Content : " + string);

        try {
            outputStream =  getContext().openFileOutput(filename, Context.MODE_PRIVATE);
            outputStream.write(string.getBytes());
            outputStream.close();
        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }

        Log.v("insert", key + " " + value);
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub

        TelephonyManager tel = (TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String idStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String port = String.valueOf((Integer.parseInt(idStr) * 2));
            myPort = port;
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }

        String portHash = "";
        try {
           portHash = genHash(idStr);
           nodeInfo.put(portHash,myPort);
           currentNode = new Node(portHash);
           currentNode.predecessor = null;
           currentNode.successor = null;
           currentNode.port = myPort;

        } catch (NoSuchAlgorithmException e) {
           Log.e(TAG, "NoSuchAlgorithmException hash error");
        }
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, NODE_JOIN_REQUEST, myPort, portHash);

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        Log.e(TAG, "MAIN query: inside query method");
        Log.e(TAG, "MAIN query: Query for selection : " +  selection);
        String colNames[] = {"key","value"};
        MatrixCursor matrixCursor = new MatrixCursor(colNames);

        try {
            // Get global list
            if(selection.equals("*")){
                if(currentNode.successor == null && currentNode.predecessor == null){
                    Log.e(TAG,"Single Node up case");
                    List<String> allGlobalFile = getAllFiles();
                    for(String file : allGlobalFile){
                        String colVal[] = getSelectionValue(file);
                        matrixCursor.addRow(colVal);
                    }
                    return  matrixCursor;
                }

                String sucPort = currentNode.successor.port;
                String  message = QUERY_ALL_DATA + DELIMETER + "@" + DELIMETER + myPort;
                try {
                    matrixCursor = new Task().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY_ALL_DATA, myPort, message, sucPort).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                Log.e(TAG, "MAIN QUERY : Returning Result matrixcursor");
            }else // get local list
                if(selection.equals("@")){
                    List<String> allGlobalFile = getAllFiles();
                    for(String file : allGlobalFile){
                        String colVal[] = getSelectionValue(file);
                        matrixCursor.addRow(colVal);
                    }
            }else{
                    String keyHash = genHash(selection);
                    if(currentNode.successor == null && currentNode.predecessor == null){
                        Log.e(TAG,"Single Node up case");
                        String colVal[] = getSelectionValue(selection);
                        matrixCursor.addRow(colVal);
                        return  matrixCursor;
                    }
                    Log.e(TAG, "Query: KeyHash ::" + keyHash +  "  for key: " +  selection);
                    if(currentNode.id.compareTo(currentNode.predecessor.id) < 0 && keyHash.compareTo(currentNode.id) > 0 &&
                            keyHash.compareTo(currentNode.predecessor.id)>0){
                        Log.e(TAG, "KeyHash is greater than largest Key so Data will be [here]");

                        String colVal[] = getSelectionValue(selection);
                        matrixCursor.addRow(colVal);
                    }
                    else if(currentNode.id.compareTo(currentNode.predecessor.id) < 0 && keyHash.compareTo(currentNode.id) < 0){
                        Log.e(TAG, "KeyHash is less than smallest Key so Data will be [here]");

                        String colVal[] = getSelectionValue(selection);
                        matrixCursor.addRow(colVal);
                    }
                    else if(keyHash.compareTo(currentNode.id) < 0 && keyHash.compareTo(currentNode.predecessor.id) > 0){
                        Log.e(TAG, "KeyHash is less than current Node id  and Greater than previous so Data will be here");

                        String colVal[] = getSelectionValue(selection);
                        matrixCursor.addRow(colVal);
                    }
                    else {
                        Log.e(TAG, "MAIN QUERY : QUERY OTHER_NODE : KeyHash: " + keyHash + " is greater than current Node id " + currentNode.id);
                        // send the data to next node.
                        String sucPort = currentNode.successor.port;
                        String dataMessage =  QUERY_DATA + DELIMETER + selection + DELIMETER  + myPort;

                        try {
                            matrixCursor = new Task().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, QUERY_DATA, myPort, dataMessage, sucPort).get();
                            Log.e(TAG, "MAIN QUERY : Returning Result matrixcursor");
                        } catch (InterruptedException e) {
                            Log.e(TAG, "InterruptedException in query Task");
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                            Log.e(TAG, "ExecutionException in query Task");
                        }

                    }
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "NoSuchAlgorithmException query method");
            e.printStackTrace();
        }
        Log.v("query", selection);
        return matrixCursor;
    }

    private String[] getSelectionValue(String selection) throws IOException {
        FileInputStream fileInputStream = getContext().openFileInput(selection);
        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line = bufferedReader.readLine();
        Log.v("Line Read from file", line);
        String colVal[] = {selection,line};
        return colVal;
    }

    List<String> getAllFiles(){
        List<String> allFiles = new ArrayList<String>();
        File folder = new File(String.valueOf(getContext().getFilesDir()));
        for (final File fileEntry : folder.listFiles()) {
            allFiles.add(fileEntry.getName());
            }

        return allFiles;
    }
    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    public String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
        }

        @Override
        protected Void doInBackground(ServerSocket... serverSockets) {

            ServerSocket serverSocket = serverSockets[0];
            while (true) {
                try {
                    Log.e(TAG, "Ready to accept Connection");
                    Socket socket = serverSocket.accept();

                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                    String message = bufferedReader.readLine();
                    Log.e(TAG, "SERVER : Received Message : " + message);
                    String [] msgToken = message.split(DELIMETER);

                    if(msgToken[0].equals(NODE_JOIN_REQUEST)) {
                        Log.e(TAG, "SERVER: Inside Node Join Request");
                        String reqPort = msgToken[1];
                        String reqPortHash = msgToken[2];
                        if(!reqPort.equals(myPort)){
                            //Broadcast topology to everyone

                            nodeInfo.put(reqPortHash,reqPort);
                            String topology = getTopologyInfo(nodeInfo);
                            Log.e(TAG, "SERVER: topology constructed : "+ topology);
                            String topoToken[] = topology.split(DELIMETER);
                            for(int i = 0; i < topoToken.length; i++){
                                String node = topoToken[i];
                                Log.e(TAG, "SERVER: topoToken["+i+"] :" + node);
                                String cPort = node.split(NODE_DEL)[1];
                                Socket cSocket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(cPort));

                                PrintStream printStream = new PrintStream(cSocket.getOutputStream());
                                printStream.println(UPDATE + DELIMETER + topology);
                                Log.e(TAG, "SERVER: Sending topology : "+topology + " To Port:" + cPort);
                            }

                        }
                        else
                        {
                            Log.e(TAG,"SERVER: same node");
                        }
                    }
                    //update Predecessor and Successor
                    if(msgToken[0].equals(UPDATE)) {
                        Log.e(TAG,"SERVER: inside UPDATE");
                        int totalNode = msgToken.length-1;
                        for(int i = 1; i <= totalNode; i++){
                            Log.e(TAG,"SERVER: Node info: " + msgToken[i]);
                            String id = "";
                            String port = "";
                            if(msgToken[i].contains(myPort)){
                                if(i == 1){
                                    String nextNode = msgToken[i+1];
                                    String lastNode = msgToken[totalNode];
                                    Log.e(TAG,"SERVER: [1] successor i == 1: " + nextNode +  " predecessor " + lastNode);
                                    id = nextNode.split(NODE_DEL)[0];
                                    port  = nextNode.split(NODE_DEL)[1];
                                    Node sNode = new Node(id);
                                    sNode.port = port;
                                    currentNode.successor = sNode; //nextNode.split(NODE_DEL)[0];
                                    id = lastNode.split(NODE_DEL)[0];
                                    port = lastNode.split(NODE_DEL)[1];
                                    Node pNode = new Node(id);
                                    pNode.port = port;
                                    currentNode.predecessor = pNode;//lastNode.split(NODE_DEL)[0];
                                    Log.e(TAG,"SERVER: [2] successor i == 1: " + currentNode.successor.id +  " predecessor " + currentNode.predecessor.id);
                                }
                                else if(i == totalNode){
                                    String nextNode = msgToken[1];
                                    String previousNode = msgToken[i-1];
                                    id = nextNode.split(NODE_DEL)[0];
                                    port  = nextNode.split(NODE_DEL)[1];
                                    Node sNode = new Node(id);
                                    sNode.port = port;
                                    Log.e(TAG,"SERVER: [1] successor (i == lastNode:) " + nextNode +  " predecessor Node " + previousNode);
                                    currentNode.successor = sNode ;//nextNode.split(NODE_DEL)[0];
                                    id = previousNode.split(NODE_DEL)[0];
                                    port = previousNode.split(NODE_DEL)[1];
                                    Node pNode = new Node(id);
                                    pNode.port = port;
                                    currentNode.predecessor = pNode ;//previousNode.split(NODE_DEL)[0];
                                    Log.e(TAG,"SERVER: [2] successor (i == lastNode:) " + currentNode.successor.id+  " predecessor Node " + currentNode.predecessor.id);
                                }
                                else{
                                    String nextNode = msgToken[i+1];
                                    String previousNode = msgToken[i-1];
                                    Log.e(TAG,"SERVER: [1] successor " + nextNode +  " predecessor Node " + previousNode);
                                    id = nextNode.split(NODE_DEL)[0];
                                    port  = nextNode.split(NODE_DEL)[1];
                                    Node sNode = new Node(id);
                                    sNode.port = port;
                                    currentNode.successor = sNode;//nextNode.split(NODE_DEL)[0];
                                    id = previousNode.split(NODE_DEL)[0];
                                    port = previousNode.split(NODE_DEL)[1];
                                    Node pNode = new Node(id);
                                    pNode.port = port;
                                    currentNode.predecessor = pNode ; //previousNode.split(NODE_DEL)[0];
                                    Log.e(TAG,"SERVER: [1] successor " + currentNode.successor.id +  " predecessor Node " + currentNode.predecessor.id);
                                }
                                Log.e(TAG, "Update Complete: " + currentNode.predecessor.id + "<-- " + currentNode.id
                                        + " -->" + currentNode.successor.id);
                            }
                        }
                    }
                    if(msgToken[0].equals(INSERT_DATA)){
                        String key = msgToken[1];
                        String value = msgToken[2];
                        //ContentResolver contentResolver = getContext().getContentResolver();
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                        ContentValues cv = new ContentValues();
                        cv.put(KEY_FIELD , key);
                        cv.put(VALUE_FIELD , value);
                        Log.e(TAG, "MAIN INSERT: SERVER: Insert Called from server task with Key:Value = " + key + ":" + value);
                        insert(mUri,cv);
                    }
                    if(msgToken[0].equals(QUERY_DATA)){

                        String selKey = msgToken[1];
                        String fromPort = msgToken[2];
                        Log.e(TAG, "MAIN QUERY : SERVER [QUERY_DATA] condition: with selKey : " + selKey);
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

                        Cursor resultCursor = query(mUri, null, selKey, null, null);
                        if (resultCursor == null) {
                            Log.e(TAG, "MAIN QUERY : SERVER [QUERY_DATA]: Result resultCursor null");
                        }
                        else{
                            String cursorMessages = getMessageFromCursor(resultCursor);

                            PrintStream printStream = new PrintStream(socket.getOutputStream());
                            printStream.println(cursorMessages);
                            Log.e(TAG, "MAIN QUERY : SERVER [QUERY_DATA]: Sending query result to client: "  + cursorMessages);
                        }
                    }
                    if(msgToken[0].equals(QUERY_ALL_DATA)){
                        //String dataMessage = QUERY_ALL_DATA + "@" + DELIMETER + myPort;
                        String selKey = msgToken[1];
                        String sendoPort = msgToken[2];
                        Log.e(TAG, "MAIN QUERY : SERVER [QUERY_ALL_DATA] condition: with selKey : " + selKey);
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

                        Cursor resultCursor = query(mUri, null, selKey, null, null);
                        if (resultCursor == null) {
                            Log.e(TAG, "MAIN QUERY : SERVER [QUERY_ALL_DATA]: Result resultCursor null");
                        }
                        else{
                            String cursorMessages = getMessageFromCursor(resultCursor);
                            // Add nextNode info to the message
                            cursorMessages = cursorMessages + DELIMETER + currentNode.successor.port;
                            PrintStream printStream = new PrintStream(socket.getOutputStream());
                            printStream.println(cursorMessages);
                            Log.e(TAG, "MAIN QUERY : SERVER [QUERY_ALL_DATA]: Sending query result to client: "  + cursorMessages);
                        }
                    }
                    if(msgToken[0].equals(DELETE_DATA)){
                        //DELETE_DATA + DELIMETER + selection + DELIMETER  + myPort;
                        String selKey = msgToken[1];
                        String senderPort = msgToken[2];
                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                        delete(mUri, selKey, null);
                        String delMsg = "Deleted Key :" + selKey;
                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(delMsg);
                        Log.e(TAG, "MAIN DELETE : SERVER [DELETE_DATA]: Sending query result to client: "  + delMsg);

                    }
                    if(msgToken[0].equals(DELETE_ALL_DATA)){
                     //DELETE_ALL_DATA + DELIMETER + "@" + DELIMETER + myPort;
                      String selKey = msgToken[1];
                      String senderPort = msgToken[2];

                        Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                        delete(mUri, selKey, null);
                        String delMsg = "Deleted Key :" + selKey;
                        // Add nextNode info to the message
                        delMsg = delMsg + DELIMETER + currentNode.successor.port;
                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(delMsg);
                        Log.e(TAG, "MAIN DELETE : SERVER [DELETE_DATA]: Sending query result to client: "  + delMsg);
                    }
                    socket.close();
                    Log.e(TAG, "socket closed");
                } catch (IOException e) {
                    Log.e(TAG,"SERVER: IOException");
                }
            }
        }

        private String getMessageFromCursor(Cursor resultCursor){
            StringBuilder sb = new StringBuilder();
            while(resultCursor.moveToNext()){
                int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                String returnKey = resultCursor.getString(keyIndex);
                String returnValue = resultCursor.getString(valueIndex);
                sb.append(returnKey+NODE_DEL+returnValue).append(DELIMETER);
                Log.e(TAG, "SERVER: query messge is : " + returnKey + "//" + returnValue);
            }
            if(sb.length() >2){
                return sb.toString().substring(0, sb.length()-2);
            }
           else{
                Log.e(TAG, "method getMessageFromCursor empty cursor");
                return "";
            }
        }
        private String getTopologyInfo(Map<String, String> nodeInfo) {
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<String,String> entry : nodeInfo.entrySet()){
                String hValue = entry.getKey();
                String pValue = entry.getValue();
                String nodeValue = hValue + NODE_DEL + pValue;
                sb.append(nodeValue).append(DELIMETER);
            }
            Log.e(TAG, "SERVER: Topology " + sb.toString());
            return sb.toString().substring(0, sb.length()-2);
        }

        @Override
        protected void onProgressUpdate(String... values) {



        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            try {
                String msgType = msgs[0];
                Log.e(TAG, "Message Type : " + msgType);
                if(msgType.equals(NODE_JOIN_REQUEST)){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(HEAD_NODE_PORT));


                    String myPort = msgs[1];
                    String portHash = msgs[2];

                    String joinRequestMsg = msgType + "##" + myPort + "##" + portHash;
                    // Write message to socket output stream
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    printStream.println(joinRequestMsg);
                    Log.e(TAG, "CLIENT: Sending message : "+joinRequestMsg + " To port: " + HEAD_NODE_PORT);
                }
                if(msgType.equals(INSERT_DATA)){
                    String myPort = msgs[1];
                    String msg = msgs[2];
                    String port = msgs[3];
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(port));

                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    printStream.println(msg);
                    Log.e(TAG, "CLIENT: Sending message : "+ msg + " To Successor port: " + port);
                }
                // socket.close();
            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class Task extends AsyncTask<String, Void, MatrixCursor> {
        @Override
        protected MatrixCursor doInBackground(String... msgs) {
            //String dataMessage =  QUERY_DATA + DELIMETER + selection + DELIMETER  + myPort;
            //QUERY_DATA, myPort, dataMessage, sucPort
            try {
                String msgType = msgs[0];
                if(msgType.equals(QUERY_DATA)){
                    String sucPort = msgs[3];
                    String dataMessage =msgs[2];
                    Log.e(TAG, "Message Type : " + msgType + " dataMessage: "+ dataMessage + " sucPort: "+ sucPort);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(sucPort));

                    String queryMessage = dataMessage;
                    // Write message to socket output stream
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    printStream.println(queryMessage);
                    Log.e(TAG, "MAIN QUERY : TASK : Sending message : " + queryMessage + " To port: " + sucPort);

                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    //proposedSeqNo + FROM process + for msgID
                    String queryResult = bufferedReader.readLine();
                    Log.e(TAG, "MAIN QUERY : TASK : Received message : " + queryResult);

                    String colNames[] = {"key","value"};
                    MatrixCursor matrixCursor = new MatrixCursor(colNames);

                    /// return result cursor from here
                    matrixCursor = createMatrixCursor(matrixCursor, queryResult);
                    Log.e(TAG, "MAIN QUERY : TASK : Returning matrix cursor with count : " + matrixCursor.getCount());
                    return matrixCursor;
                }
                if(msgType.equals(QUERY_ALL_DATA)){
                    //AsyncTask.SERIAL_EXECUTOR, QUERY_ALL_DATA, myPort, message,sucPort
                    //String  message = QUERY_ALL_DATA + DELIMETER + selection + DELIMETER + myPort;
                    String dataMessage = msgs[2];
                    String sucPort = msgs[3];
                    // loop through all the node and all for there "@" data
                    Log.e(TAG, "Message Type : " + msgType + " dataMessage: "+ dataMessage + " sucPort: "+ sucPort);
                    boolean isDone = false;
                    String colNames[] = {"key","value"};
                    MatrixCursor matrixCursor = new MatrixCursor(colNames);

                    while(!isDone){
                        Log.e(TAG, "MAIN QUERY : TASK Inside While loop");
                            if(sucPort.equals(myPort)){
                                isDone = true;
                                Log.e(TAG, "While loop Complete after this iterations: SuccPort: " + sucPort + " MyPort:" + myPort);
                            }
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(sucPort));

                            String queryMessage = dataMessage;
                            // Write message to socket output stream
                            PrintStream printStream = new PrintStream(socket.getOutputStream());
                            printStream.println(queryMessage);
                            Log.e(TAG, "MAIN QUERY [@]: TASK : Sending message : " + queryMessage + " To port: " + sucPort);

                            InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                            //proposedSeqNo + FROM process + for msgID
                            String queryResult = bufferedReader.readLine();
                            Log.e(TAG, "MAIN QUERY [@]: TASK : Received message : " + queryResult);
                            String nextPortInfo = queryResult.substring(queryResult.length() - 5);
                            if(queryResult.length() > 7) {
                                queryResult = queryResult.substring(0, queryResult.length() - 7);
                                Log.e(TAG, "MAIN QUERY [@]: portInfo " + nextPortInfo + " QueryResult: " + queryResult);
                                /// return result cursor from here
                                matrixCursor = createMatrixCursor(matrixCursor, queryResult);
                                Log.e(TAG, "MAIN QUERY [@]: TASK : Returning matrix cursor with count : " + matrixCursor.getCount());
                            }
                           // nextNode = nextNode.successor;
                            sucPort =nextPortInfo;
                    }
                return matrixCursor;
                }
                if(msgType.equals(DELETE_DATA)){
                    //DELETE_DATA + DELIMETER + selection + DELIMETER  + myPort;
                    //AsyncTask.SERIAL_EXECUTOR, DELETE_DATA, deleteMessage, sucPort
                    String deleteMessage = msgs[1];
                    String sucPort = msgs[2];

                    Log.e(TAG, "Message Type : " + msgType + " deleteMessage: "+ deleteMessage + " sucPort: "+ sucPort);

                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(sucPort));

                    // Write message to socket output stream
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    printStream.println(deleteMessage);
                    Log.e(TAG, "MAIN DELETE : TASK : Sending delete message : " + deleteMessage + " To port: " + sucPort);

                    InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                    String queryResult = bufferedReader.readLine();
                    Log.e(TAG, "MAIN DELETE: TASK : Received message : " + queryResult);
                }
                if(msgType.equals(DELETE_ALL_DATA)){
                    //AsyncTask.SERIAL_EXECUTOR, DELETE_ALL_DATA, delAllMessage, sucPort
                    //DELETE_ALL_DATA + DELIMETER + "@" + DELIMETER + myPort;
                    String delMessage = msgs[1];
                    String sucPort = msgs[2];

                    // loop through all the node and all for there "@" data
                    Log.e(TAG, "Message Type : " + msgType + " deleteMessage: "+ delMessage + " sucPort: "+ sucPort);
                    boolean isDone = false;

                    while(!isDone){
                        Log.e(TAG, "MAIN DELETE : TASK Inside While loop");
                        if(sucPort.equals(myPort)){
                            isDone = true;
                            Log.e(TAG, "While loop Complete after this iterations: SuccPort: " + sucPort + " MyPort:" + myPort);
                        }
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(sucPort));

                                               // Write message to socket output stream
                        PrintStream printStream = new PrintStream(socket.getOutputStream());
                        printStream.println(delMessage);
                        Log.e(TAG, "MAIN QUERY [@]: TASK : Sending delete message : " + delMessage + " To port: " + sucPort);

                        InputStreamReader inputStreamReader = new InputStreamReader(socket.getInputStream());
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                        String queryResult = bufferedReader.readLine();
                        //queryResult = delMsg + DELIMETER + currentNode.successor.port;
                        Log.e(TAG, "MAIN DELETE [@]: TASK : Received message : " + queryResult);
                        String nextPortInfo = queryResult.substring(queryResult.length() - 5);
                        sucPort =nextPortInfo;
                    }
                }
            }
                 catch (UnknownHostException e) {
                    Log.e(TAG, "ClientTask UnknownHostException");
                } catch (IOException e) {
                    Log.e(TAG, "ClientTask socket IOException");
                }


                Log.e(TAG, "TASK:  INSIDE doBackground ");
            return null;
        }

        private MatrixCursor createMatrixCursor(MatrixCursor matrixCursor, String queryResult) {
            String[] resultToken = queryResult.split(DELIMETER);
            Log.e(TAG, "MAIN QUERY Inside createMatrixCursor method and no of messg : = " + resultToken.length);
            for(int i =0 ; i < resultToken.length; i++){
                String message = resultToken[i];
                String colVal[] = {message.split(NODE_DEL)[0],message.split(NODE_DEL)[1]};
                matrixCursor.addRow(colVal);
            }
        return matrixCursor;
        }
    }
}
