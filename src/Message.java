import java.awt.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;


public class Message implements Serializable {
    MessageType msgType;
    public String message;
    public ArrayList<Integer> clock;
    public ArrayList<NodeStatus> nodeStatuses;
    public ArrayList<NodeState> nodeStates;

    public String nodeColor;

    public Message(ArrayList<NodeState> nodeStates) {
        this.nodeStates = nodeStates;
    }

    // Constructor
    public Message(String msg) {
        message = msg;
    }

    public Message(String message, ArrayList<Integer> clock) {
        this.message = message;
        this.clock = clock;
    }

    public Message(String message, ArrayList<Integer> clock, MessageType msgType) {
        this.msgType = msgType;
        this.message = message;
        this.clock = clock;
    }

    public Message(MessageType msgType, String message, ArrayList<Integer> clock, String nodeColor) {
        this.msgType = msgType;
        this.message = message;
        this.clock = clock;
        this.nodeColor = nodeColor;
    }

    public Message(MessageType msgType) {
        this.msgType = msgType;
    }

    public Message(MessageType msgType, ArrayList<Integer> clock) {
        this.msgType = msgType;
        this.clock = clock;
    }

    public Message(ArrayList<Integer> clock, ArrayList<NodeStatus> nodeStatuses) {
        this.clock = clock;
        this.nodeStatuses = nodeStatuses;
    }


    // Convert current instance of Message to ByteBuffer in order to send message over SCTP
    public ByteBuffer toByteBuffer() throws Exception
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(this);
        oos.flush();

        ByteBuffer buf = ByteBuffer.allocateDirect(bos.size());
        buf.put(bos.toByteArray());

        oos.close();
        bos.close();

        // Buffer needs to be flipped after writing
        // Buffer flip should happen only once
        buf.flip();
        return buf;
    }


    public static Message fromByteBuffer(ByteBuffer buf) throws Exception
    {
        // Buffer needs to be flipped before reading
        // Buffer flip should happen only once
        buf.flip();
        byte[] data = new byte[buf.limit()];
        buf.get(data);
        buf.clear();

        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Message msg = (Message) ois.readObject();

        bis.close();
        ois.close();

        return msg;
    }
}
