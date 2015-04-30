package edu.buffalo.cse.cse486586.simpledynamo;
/**
 * Created by sherlock on 4/20/15.
 */

class Node{

    String port;
    String hashPort;
    boolean isAlive;

    Node next;
    Node previous;

    public Node(String key, String hashPort)
    {
        this.port = key;
        this.hashPort = hashPort;
        next = null;
        previous = null;
        isAlive = true;
    }
}
public class ChordLinkList {

    Node head = null;

    public ChordLinkList(Node n1, Node n2, Node n3, Node n4, Node n5, String port)
    {
        if(n1.port.equals(port))
        {
            head = n1;
            return;
        }
        if(n2.port.equals(port))
        {
            head = n2;
            return;
        }
        if(n3.port.equals(port))
        {
            head = n3;
            return;
        }
        if(n4.port.equals(port))
        {
            head = n4;
            return;
        }
        if(n5.port.equals(port))
        {
            head = n5;
            return;
        }
    }
    public ChordLinkList(String port, String hashPort)
    {
        head = new Node(port, hashPort);
        head.next = head;
        head.previous = head;
    }

    public void addNode(Node node, String port, String hashPort, String position)
    {
        Node tempNode = new Node(port, hashPort);
        if(position.equals(DynamoResources.NEXT))
        {
            Node next = node.next;
            node.next = tempNode;
            tempNode.next = next;
            next.previous = tempNode;
            tempNode.previous = node;
        }

        if(position.equals(DynamoResources.PREVIOUS))
        {
            Node previous = node.previous;
            node.previous = tempNode;
            previous.next = tempNode;
            tempNode.next = node;
            tempNode.previous = previous;
        }

    }

    public String getPreferenceList(String coordinator)
    {
        Node node = head;

        while(true)
        {
            if(node.port.equals(coordinator))
                break;
            node = node.next;
            if(node == head)
                break;
        }

        return node.next.port + DynamoResources.valSeparator + node.next.next.port;
    }

    public String[] getPreferenceListArray(String coordinator)
    {
        Node node = head;

        while(true)
        {
            if(node.port.equals(coordinator))
                break;
            node = node.next;
            if(node == head)
                break;
        }

        return new String[]{node.next.port,node.next.next.port };
//        return node.next.port + DynamoResources.valSeparator + node.next.next.port;
    }

    public synchronized boolean getLifeStatus(String port)
    {
        Node node = head;
        while (true)
        {
            if(node.port.equals(port))
                break;
            node = node.next;
        }
        return node.isAlive;
    }

    public boolean existsInChain(String port)
    {
        Node node = head;
        while (true)
        {
            if(node.port.equals(port))
                return true;
            node = node.next;
            if(node == head)
                break;
        }
        return false;
    }

    public synchronized void setLifeStatus(String port, boolean status)
    {
        Node node = head;
        while (true)
        {
            if(node.port.equals(port))
                break;
            node = node.next;
        }
        node.isAlive = status;
    }
    public boolean haveIgot(String port)
    {
        Node node = head;

        while (true)
        {
            if(node.port.equals(port))
                break;
             node = node.next;
        }

        if(node.next.equals(head.port) || node.next.next.port.equals(head.port))
            return true;
        else return false;
    }

    public int size()
    {
        Node node = head;
        int count = 0;
        while(true && node != null)
        {
            node = node.next;
            count++;
            if(node == head)
                break;
        }

        return count;
    }


    public String printRing()
    {
        Node node = head;
        String result = "";
        while(true)
        {
            if(node != null)
                result = result + node.port;

            if(node.next == head)
                break;
            result = result + "-->";
            node = node.next;
        }
        return result;
    }
}
