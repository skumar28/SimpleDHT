package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by Sandeep on 4/2/2018.
 */

public class Node {
    String id;
    Node predecessor;
    Node successor;
    String port;

    Node(String node_id){
        this.id = node_id;
    }
}
