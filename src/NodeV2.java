import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;

import javax.swing.plaf.IconUIResource;
import java.io.*;
import java.lang.reflect.Array;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

public class NodeV2 {
    private Integer nodeNum;
    private String nodeHost;
    private String systemName;
    private String nodeColor;
    private Integer minPerActive;
    private Integer maxPerActive;
    private Integer minSendDelay;
    private Integer snapShotDelay;
    private Integer maxNumber;
    private NodeStatus nodeStatus = NodeStatus.ACTIVE;
    private SctpServerChannel listenerChannel;
    private ArrayList<String> messagesRecd = new ArrayList<String>();
    private HashMap<Integer, String> nodeMap = new HashMap<Integer, String>();

    private HashMap<Integer, Integer> portMap = new HashMap<Integer, Integer>();
    private ArrayList<Integer> connectedNodes = new ArrayList<>();

    private ArrayList<String> messageQueue = new ArrayList<String>();
    private ArrayList<Sender> senderThreads = new ArrayList<Sender>();
    private Integer sentMessages = 0;

    private ArrayList<ListenerHandler> listenerHandlersList = new ArrayList<ListenerHandler>();
    private ArrayList<Integer> vectorClock;
    private String outputFileName;

    private int recRedAppMsg = 0;

    private ArrayList<Integer> sendClock = new ArrayList<>();

    public int getRecRedAppMsg() {
        return recRedAppMsg;
    }

    public void setRecRedAppMsg(int recRedAppMsg) {
        this.recRedAppMsg = recRedAppMsg;
    }

    public ArrayList<Integer> getSendClock() {
        return sendClock;
    }

    public void setSendClock(ArrayList<Integer> sendClock) {
        //System.out.println(" SETTER CALLED ");
        this.sendClock = sendClock;
    }

    public NodeV2() {

    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    public String getNodeColor() {
        return nodeColor;
    }

    public void setNodeColor(String nodeColor) {
        this.nodeColor = nodeColor;
    }

    public ArrayList<ListenerHandler> getListenerHandlersList() {
        return listenerHandlersList;
    }

    public void setListenerHandlersList(ArrayList<ListenerHandler> listenerHandlersList) {
        this.listenerHandlersList = listenerHandlersList;
    }

    public Integer getNodeNum() {
        return nodeNum;
    }

    public void setNodeNum(Integer nodeNum) {
        this.nodeNum = nodeNum;
    }

    public String getNodeHost() {
        return nodeHost;
    }

    public void setNodeHost(String nodeHost) {
        this.nodeHost = nodeHost;
    }

    public Integer getMinPerActive() {
        return minPerActive;
    }

    public void setMinPerActive(Integer minPerActive) {
        this.minPerActive = minPerActive;
    }

    public Integer getMaxPerActive() {
        return maxPerActive;
    }

    public void setMaxPerActive(Integer maxPerActive) {
        this.maxPerActive = maxPerActive;
    }

    public Integer getMinSendDelay() {
        return minSendDelay;
    }

    public void setMinSendDelay(Integer minSendDelay) {
        this.minSendDelay = minSendDelay;
    }

    public Integer getSnapShotDelay() {
        return snapShotDelay;
    }

    public void setSnapShotDelay(Integer snapShotDelay) {
        this.snapShotDelay = snapShotDelay;
    }

    public Integer getMaxNumber() {
        return maxNumber;
    }

    public void setMaxNumber(Integer maxNumber) {
        this.maxNumber = maxNumber;
    }

    public NodeStatus getNodeStatus() {
        return nodeStatus;
    }

    public void setNodeStatus(NodeStatus nodeStatus) {
        this.nodeStatus = nodeStatus;
    }

    public ArrayList<String> getMessagesRecd() {
        return messagesRecd;
    }

    public void setMessagesRecd(ArrayList<String> messagesRecd) {
        this.messagesRecd = messagesRecd;
    }

    public HashMap<Integer, String> getNodeMap() {
        return nodeMap;
    }

    public void setNodeMap(HashMap<Integer, String> nodeMap) {
        this.nodeMap = nodeMap;
    }

    public HashMap<Integer, Integer> getPortMap() {
        return portMap;
    }

    public void setPortMap(HashMap<Integer, Integer> portMap) {
        this.portMap = portMap;
    }

    public ArrayList<Integer> getConnectedNodes() {
        return connectedNodes;
    }

    public void setConnectedNodes(ArrayList<Integer> connectedNodes) {
        this.connectedNodes = connectedNodes;
    }

    public ArrayList<String> getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(ArrayList<String> messageQueue) {
        this.messageQueue = messageQueue;
    }

    public ArrayList<Sender> getSenderThreads() {
        return senderThreads;
    }

    public void setSenderThreads(ArrayList<Sender> senderThreads) {
        this.senderThreads = senderThreads;
    }

    public Integer getSentMessages() {
        return sentMessages;
    }

    public void setSentMessages(Integer sentMessages) {
        this.sentMessages = sentMessages;
    }

    public ArrayList<Integer> getVectorClock() {
        return vectorClock;
    }

    public void setVectorClock(ArrayList<Integer> vectorClock) {
        this.vectorClock = vectorClock;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String SystemName) {
        this.systemName = SystemName;
    }

    @Override
    public String toString() {
        return "Node{" +
                "nodeNum=" + nodeNum +
                ", nodeHost='" + nodeHost + '\'' +
                ", minPerActive=" + minPerActive +
                ", maxPerActive=" + maxPerActive +
                ", minSendDelay=" + minSendDelay +
                ", snapShotDelay=" + snapShotDelay +
                ", maxNumber=" + maxNumber +
                ", nodeStatus=" + nodeStatus +
                ", messagesRecvd=" + messagesRecd +
                ", nodeMap=" + nodeMap +
                ", portMap=" + portMap +
                ", connectedNodes=" + connectedNodes +
                ", messageQueue=" + messageQueue +
                ", senderThreads=" + senderThreads +
                ", sentMessages=" + sentMessages +
                '}';
    }

    public SctpServerChannel getListenerChannel() {
        return listenerChannel;
    }

    public void setListenerChannel(SctpServerChannel listenerChannel) {
        this.listenerChannel = listenerChannel;
    }

    public static void main(String[] args) {
        String str = String.join(" ", args);
        String[] input = str.split("-");
        NodeV2 node = new NodeV2();
        ArrayList<Integer> neighbours = new ArrayList<>();
        int num = 0;
        node.setNodeColor("W");
        try {
            node.setSystemName(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < input.length; i++) {
            String[] temp = input[i].split(" ");
            if (i == 0) {
                num = Integer.parseInt(temp[0]);
                node.setVectorClock(new ArrayList<>(Collections.nCopies(num, 0)));
                node.setMinPerActive(Integer.parseInt(temp[1]));
                node.setMaxPerActive(Integer.parseInt(temp[2]));
                node.setMinSendDelay(Integer.parseInt(temp[3]));
                node.setSnapShotDelay(Integer.parseInt(temp[4]));
                node.setMaxNumber(Integer.parseInt(temp[5]));
            } else {
                node.getNodeMap().put(Integer.parseInt(temp[0]), temp[1]);
                if ((node.getSystemName()).contains(temp[1])) {
                    node.setNodeHost(temp[1]);
                    node.setNodeNum(Integer.parseInt(temp[0]));
                }
                node.getPortMap().put(Integer.parseInt(temp[0]), Integer.parseInt(temp[2]));
                if (node.getNodeNum() != null && node.getNodeNum() == i - 1) {
                    for (int d = 3; d < temp.length; d++) {
                        neighbours.add(Integer.parseInt(temp[d]));
                    }
                }
            }
        }
        node.setOutputFileName("config-"+node.getNodeNum()+".out");
        if (node.getNodeNum() == 0) {
            node.setNodeStatus(NodeStatus.ACTIVE);
        } else {
            node.setNodeStatus(NodeStatus.PASSIVE);
        }
        Thread listener = new Thread() {
            public void run() {
                node.createListener();
                //System.out.println(" ** Exiting the listener thread ** ");
            }
        };
        listener.start();
        try {
            Thread.sleep(10000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (Integer i : neighbours) {
            node.createSender(i);
        }
        //System.out.println(" -- Created Senders -- ");
        new Thread() {
            public void run() {
                while (canSendMessages()) {
                    if (node.getNodeStatus() == NodeStatus.ACTIVE) {
                        node.sendMessages();
                    }
                }
                //System.out.println(" ** Exiting the message sending thread ** ");
            }

            public boolean canSendMessages() {
                synchronized (node) {
                    return (node.getSentMessages() < node.getMaxNumber());
                }
            }
        }.start();

        // Thread for chandy babai
        if (node.getNodeNum() == 0) {
            new Thread() {
                public void run() {
                    while (true) {
                        if (node.getNodeStatus().equals(NodeStatus.TERMINATED)) {
                            break;
                        }
                        try {
                            //System.out.println(" -- Sleeping before initiating snapshot algorithm -- ");
                            Thread.sleep(node.getSnapShotDelay());
                            synchronized (node) {
                               // node.setNodeColor("R");
                                node.setRecRedAppMsg(1);
                                ArrayList<NodeState> states = new ArrayList<>();
                                ArrayList<Integer> clock = new ArrayList<>(node.getVectorClock());
                                node.setSendClock(clock);
                                NodeState nodeState = new NodeState(node.getNodeNum(), clock, node.getNodeStatus());
                                System.out.println(" added state " + nodeState.toString());
                                states.add(nodeState);
                                //System.out.println(" -- Write clock to the file -- " + node.getOutputFileName() + " clock : " + clock);
                                FileOutputStream fileOut = new FileOutputStream(node.getOutputFileName(), true);
                                PrintWriter writer = new PrintWriter(fileOut);
                                for (Integer i : clock) {
                                    writer.print(i);
                                    writer.print(" ");
                                }
                                writer.println();
                                writer.close();
                                fileOut.close();
                                
                                /*if(node.getSentMessages()> node.getMaxNumber()) {
                                    Random r = new Random();
                                    Sender t = node.getSenderThreads().get(r.nextInt(node.getSenderThreads().size()));
                                    Thread t1 = new Thread(t);
                                    t1.start();
                                } else {
                                    Thread.sleep(10000);
                                }*/
                                Thread.sleep(10000);
                                node.setNodeColor("R");
                                // ArrayList<Integer> visitedL = new ArrayList<>();
                                // visitedL.add(0);
                                states.addAll(node.sendMarkerMessages());
                                //states.add(nodeState);
                                if(node.verifyConsistency(states)) {
                                    System.out.println("^^^ States are consistent ^^^^ ");
                                } else {
                                    System.out.println("^^ states are not consistent ^^ ");
                                }
                                int terminate = 1;
                                for (NodeState state : states) {
                                    if (state.getStatus() != NodeStatus.CHANNEL_NON_EMPTY)
                                        System.out.println(state.toString());
                                    //System.out.print(" " + state.getStatus());
                                    //if (state.getStatus() == NodeStatus.ACTIVE || state.getStatus() == NodeStatus.PASSIVE) {
                                    // //System.out.println(state.getClock().toString());
                                    // }
                                }
                                //System.out.println(" ");
                                for (NodeState state : states) {
                                    if (state.getStatus().equals(NodeStatus.ACTIVE) || state.getStatus().equals(NodeStatus.CHANNEL_NON_EMPTY)) {
                                        terminate = 0;
                                        break;
                                    }
                                }
                                if (terminate == 1) {
                                    ArrayList<Integer> v = new ArrayList<>();
                                    v.add(node.getNodeNum());
                                    node.sendTerminateMessage(v);
                                    node.closeEverything();
                                    break;
                                } else {
                                    ArrayList<Integer> v = new ArrayList<>();
                                    v.add(node.getNodeNum());
                                    node.setNodeColor("W");
                                    node.sendEndSnapShotMessage(v);
                                    node.setRecRedAppMsg(0);
                                }
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    //System.out.println(" ** Exiting the snapshot algorithm thread ** ");
                }
            }.start();
        }
        //System.out.println(" ** Exiting the Main thread ** ");
    }

    private boolean verifyConsistency(ArrayList<NodeState> states) {
        ArrayList<ArrayList<Integer>> clock = new ArrayList<ArrayList<Integer>>();
        for(NodeState state : states) {
            if(state.getStatus() != NodeStatus.CHANNEL_NON_EMPTY) {
                //System.out.println( " ```` " + state.getClock().toString());
                clock.add(state.getClock());
            }
        }
        //System.out.println(" Number of clocks " + clock.size());
        int[][] intArray = clock.stream().map(  u  ->  u.stream().mapToInt(i->i).toArray()  ).toArray(int[][]::new);
        for(int i=0;i<clock.size();i++) {
            int max=0;
            for (int j=0;j<clock.size();j++) {
                max=Math.max(max,intArray[j][i]);
            }
            if(max>intArray[i][i]) return false;
        }
        return true;
    }

    private void sendTerminateMessage(ArrayList<Integer> v) {
        //System.out.println(" -- Sending Terminate Messages -- ");
        for (Sender s : getSenderThreads()) {
            if(!v.contains(s.getListenerNodeId())) {
                s.sendTerminateSignal(v);
            }
        }
    }

    public ArrayList<NodeState> sendMarkerMessages() {
        ArrayList<NodeState> list = new ArrayList<>();
        for (Sender s : getSenderThreads()) {
            //if (!visited.contains(s.getListenerNodeId())) {
            list.addAll(s.sendMarkerMessage());
            //}
        }
        return list;
    }

    private void sendEndSnapShotMessage(ArrayList<Integer> visited) {
        for (Sender s : getSenderThreads()) {
            // if (!visited.contains(s.getListenerNodeId()))
            s.sendEndSnapShot(visited);
        }
    }

    public void closeEverything() {
        //System.out.println(" -- Closing all the connections -- ");
        if (getListenerChannel() != null) {
            try {
                if(getListenerChannel().isOpen()) {
                    getListenerChannel().close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        for (Sender sender : getSenderThreads()) {
            if (sender != null) {
                sender.closeEverything();
            }
        }
        for (ListenerHandler listenerHandler : getListenerHandlersList()) {
            if(listenerHandler != null) {
                listenerHandler.closeEverything();
            }
        }
        //System.out.println(" -- Closed all the connections -- ");
    }


    private void createSender(Integer i) {
        try {
            Sender sender = new Sender(i);
            synchronized (this) {
                getSenderThreads().add(sender);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void sendMessages() {
        if (getSenderThreads().isEmpty()) return;
        Random r = new Random();
        int numOfMessages = r.nextInt(getMaxPerActive() - getMinPerActive()) + getMinPerActive();
        while (canSendMessages(numOfMessages)) {
            Sender t = getSenderThreads().get(r.nextInt(getSenderThreads().size()));
            Thread t1 = new Thread(t);
            t1.start();
            if (getNodeStatus() == NodeStatus.ACTIVE) {
                try {
                    Thread.sleep(getMinSendDelay());
                } catch (InterruptedException e) {
                    closeEverything();
                }
            }
            numOfMessages--;
        }
        synchronized (this) {
            //System.out.println(" -- Setting Node Status Passive -- ");
            setNodeStatus(NodeStatus.PASSIVE);
        }
    }

    private boolean canSendMessages(int numOfMessages) {
        return numOfMessages > 0 && (getSentMessages() < getMaxNumber()) && (getNodeStatus() != NodeStatus.PASSIVE);
    }

    private class Sender implements Runnable {

        SctpChannel senderChannel;

        Integer listenerNodeId;
        ByteBuffer buf = ByteBuffer.allocate(8192);

        public Integer getListenerNodeId() {
            return listenerNodeId;
        }

        public void setListenerNodeId(Integer listenerNodeId) {
            this.listenerNodeId = listenerNodeId;
        }

        public SctpChannel getSenderChannel() {
            return senderChannel;
        }

        public void setSenderChannel(SctpChannel senderChannel) {
            this.senderChannel = senderChannel;
        }

        public Sender(Integer listenerNodeId) {
            setListenerNodeId(listenerNodeId);
            String listenerHost = nodeMap.get(listenerNodeId);
            Integer listenerPort = portMap.get(listenerNodeId);
            try {
                setSenderChannel(SctpChannel.open(new InetSocketAddress(listenerHost, listenerPort), 0, 0));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            sendMessages();
        }

        private void sendMessages() {
            if (senderChannel.isOpen()) {
                try {
                    MessageInfo msgInfo = MessageInfo.createOutgoing(null, 0);
                    int val = getSentMessages() + 1;
                    synchronized (NodeV2.class) {
                        getVectorClock().set(getNodeNum(), getVectorClock().get(getNodeNum()) + 1);
                        setSentMessages(getSentMessages() + 1);
                    }
                    System.out.println(" -- Sending an Application Message -- " + senderChannel.getRemoteAddresses() + " clock : " + getVectorClock()
                            + " sent messages " + getSentMessages()
                            + " node color" + getNodeColor());
                    Message msg = new Message(MessageType.APPLICATION, " host : " + getNodeHost() + " Code " + val, getVectorClock(), getNodeColor());
                    senderChannel.send(msg.toByteBuffer(), msgInfo);
                } catch (Exception e) {
                    closeEverything();
                }
            }
        }

        public void sendTerminateSignal(ArrayList<Integer> v) {
            if (senderChannel.isOpen()) {
                try {
                    //System.out.println(" -- Sending Terminate Signal -- " + senderChannel.getRemoteAddresses());
                    Message msg = new Message(MessageType.TERMINATE,v);
                    senderChannel.send(msg.toByteBuffer(), MessageInfo.createOutgoing(null, 0));
                } catch (Exception e) {
                    closeEverything();
                }
            }
        }

        public ArrayList<NodeState> sendMarkerMessage() {
            ArrayList<NodeState> list = new ArrayList<>();
            if (senderChannel.isOpen()) {
                try {
                    System.out.println(" -- Sending Marker Message -- " + senderChannel.getRemoteAddresses());
                    Message m = new Message(MessageType.MARKER);
                    ///if (visited.contains(getListenerNodeId())) return list;
                    //visited.add(getListenerNodeId());
                    senderChannel.send(m.toByteBuffer(), MessageInfo.createOutgoing(null, 0));
                    senderChannel.receive(buf, null, null);
                    Message m1 = Message.fromByteBuffer(buf);
                    list = m1.nodeStates;
                } catch (Exception e) {
                    closeEverything();
                }
            }
            return list;
        }

        public void sendEndSnapShot(ArrayList<Integer> visited) {
            if (senderChannel.isOpen()) {
                try {
                    //System.out.println(" -- Sending End Snapshot Message -- " + senderChannel.getRemoteAddresses());
                    //if (visited.contains(getListenerNodeId())) return;
                    //visited.add(getListenerNodeId());
                    Message m = new Message(MessageType.END_SNAPSHOT, visited);
                    senderChannel.send(m.toByteBuffer(), MessageInfo.createOutgoing(null, 0));
                } catch (Exception e) {
                    closeEverything();
                }
            }
        }
        public void closeEverything() {
            try {
                if (senderChannel != null) {
                    if (senderChannel.isOpen()) {
                        //System.out.println(" -- Closing a sender channel -- " +  senderChannel.getRemoteAddresses());
                        senderChannel.close();
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void createListener() {
        try {
            InetSocketAddress address = new InetSocketAddress(getPortMap().get(getNodeNum()));
            setListenerChannel(SctpServerChannel.open());
            getListenerChannel().bind(address);
            //System.out.println(" -- Created a server -- ");
            while (getListenerChannel().isOpen()) {
                SctpChannel clientChannel = getListenerChannel().accept();
                ListenerHandler listenerHandler = new ListenerHandler(clientChannel);
                getListenerHandlersList().add(listenerHandler);
                Thread thread = new Thread(listenerHandler);
                thread.start();
            }
        } catch (Exception e) {
            closeEverything();
        }
        //System.out.println(" ** Exiting the createListener ** ");
    }

    private class ListenerHandler implements Runnable {

        private SctpChannel clientChannel;
        private Integer senderNodeNum;

        private ByteBuffer byteBuffer;
        private ArrayList<Message> messageArrayList;

        public ListenerHandler(SctpChannel clientChannel) {
            this.clientChannel = clientChannel;
            setByteBuffer(ByteBuffer.allocate(8192));
            messageArrayList = new ArrayList<>();
        }

        public ByteBuffer getByteBuffer() {
            return byteBuffer;
        }

        public void setByteBuffer(ByteBuffer byteBuffer) {
            this.byteBuffer = byteBuffer;
        }

        public SctpChannel getClientChannel() {
            return clientChannel;
        }

        public void setClientChannel(SctpChannel clientChannel) {
            this.clientChannel = clientChannel;
        }

        public Integer getSenderNodeNum() {
            return senderNodeNum;
        }

        public void setSenderNodeNum(Integer senderNodeNum) {
            this.senderNodeNum = senderNodeNum;
        }

        @Override
        public void run() {
            while (clientChannel.isOpen()) {
                try {
                    clientChannel.receive(getByteBuffer(), null, null);
                    Message messageReceived = Message.fromByteBuffer(getByteBuffer());
                    MessageType msgType = messageReceived.msgType;
                    if (msgType.equals(MessageType.APPLICATION)) {
                        System.out.println(" -- Received an Application Message -- " +  clientChannel.getRemoteAddresses() + " clock " + getVectorClock().toString() + " Message clock "
                                + messageReceived.clock + " node color  = " + getNodeColor() + " message color = "
                                + messageReceived.nodeColor
                                + " received red application message " + getRecRedAppMsg());
                        if(getNodeColor().equals("W") && messageReceived.nodeColor.equals("W")) {
                            if (messageReceived.message == null) {
                                closeEverything();
                                break;
                            } else {
                                for(Message message : messageArrayList) {
                                    receiveApplicationMessage(message);
                                }
                                receiveApplicationMessage(messageReceived);
                            }
                        } else {
                            //System.out.println(" --  adding message to the list -- " + " " +  clientChannel.getRemoteAddresses());
                            if((messageReceived.nodeColor.equals("R")) && getNodeColor().equals("W")) {
                                ArrayList<Integer> temp = new ArrayList<>(getVectorClock());
                                setSendClock(temp);
                                //setRecRedAppMsg(1);
                                System.out.println( " send clock " + getSendClock());
                                setNodeColor("R");
                            }
                            if((getNodeColor().equals("R")) && (messageReceived.nodeColor.equals("W"))) {
                                messageArrayList.add(messageReceived);
                            }
                            receiveApplicationMessage(messageReceived);
                        }
                    } else if (msgType.equals(MessageType.MARKER)) {
                        System.out.println(" -- Received an Marker Message -- " +  clientChannel.getRemoteAddresses() + " node status " + getNodeStatus() + " node color " + getNodeColor());
                        if (getNodeColor().equals("R") && getRecRedAppMsg() == 1) {
                            ArrayList<NodeState> list = new ArrayList<>();
                            if(!messageArrayList.isEmpty()) {
                                NodeState nodeState = new NodeState(getNodeNum(), getVectorClock(), NodeStatus.CHANNEL_NON_EMPTY);
                                list.add(nodeState);
                                System.out.println( " state added " + nodeState.toString());
                            }
                            Message m = new Message(list);
                            clientChannel.send(m.toByteBuffer(), MessageInfo.createOutgoing(null, 0));
                        } else {
                           /* Random r = new Random();
                            Sender t = getSenderThreads().get(r.nextInt(getSenderThreads().size()));
                            Thread t1 = new Thread(t);
                            t1.start();*/
                            //Thread.sleep(10000);
                            setRecRedAppMsg(1);
                            ArrayList<NodeState> list = new ArrayList<>();
                            synchronized (NodeV2.class) {
                                if(getNodeColor().equals("W")) {
                                    System.out.println(" SETTING SEND CLOCK ");
                                    ArrayList<Integer> temp = new ArrayList<>(getVectorClock());
                                    setSendClock(temp);
                                    setRecRedAppMsg(1);
                                }
                                //Thread.sleep(10000);
                                setNodeColor("R");
                                //if(getRecRedAppMsg() == 0) {
                                //}
                                ArrayList<Integer> clock = getSendClock();
                                NodeState state = new NodeState(getNodeNum(), clock, getNodeStatus());
                                System.out.println( " state added " + state.toString());
                                list.add(state);
                                FileOutputStream fileOut = new FileOutputStream(getOutputFileName(), true);
                                PrintWriter writer = new PrintWriter(fileOut);
                                for (Integer i : clock) {
                                    writer.print(i);
                                    writer.print(" ");
                                }
                                writer.println();
                                writer.close();
                                fileOut.close();
                            }
                            list.addAll(sendMarkerMessages());
                            Message m = new Message(list);
                            clientChannel.send(m.toByteBuffer(), MessageInfo.createOutgoing(null, 0));
                        }
                    } else if (msgType.equals(MessageType.TERMINATE)) {
                        //System.out.println(" -- Received an Terminate Message -- " +  clientChannel.getRemoteAddresses());
                        ArrayList<Integer> visited = messageReceived.clock;
                        ////System.out.println(" visited " + (visited == null));
                        if(!visited.contains(getNodeNum())) {
                            visited.add(getNodeNum());
                        }
                        sendTerminateMessage(visited);
                        break;
                    } else if (msgType.equals(MessageType.END_SNAPSHOT)) {
                        ArrayList<Integer> visited = messageReceived.clock;
                        if (getNodeColor().equals("R")) {
                            synchronized (NodeV2.class) {
                                setNodeColor("W");
                                setRecRedAppMsg(0);
                                setSendClock(new ArrayList<>());
                            }
                            sendEndSnapShotMessage(visited);
                        }
                        //System.out.println(" ## Size of messageList  ## " + messageArrayList.size() + " Node color " + getNodeColor() + " " +  clientChannel.getRemoteAddresses());
                        messageArrayList.clear();
                    }
                } catch (Exception e) {
                    //e.printStackTrace();
                    closeEverything();
                    break;
                }
            }
            //System.out.println(" ** Exiting the ListenerHandler thread ** " );
            closeEverything();
        }

        private void receiveApplicationMessage(Message messageReceived) {
            try {
                String messageText = messageReceived.message;
                ArrayList<Integer> mClock = messageReceived.clock;
                //System.out.println(" -- Received an Application Message -- " +  clientChannel.getRemoteAddresses() + " clock " + getVectorClock().toString() + " Message clock " + mClock);
                synchronized (NodeV2.class) {
                    updateVectorClock(mClock);
                    if (getSentMessages() < getMaxNumber()) {
                        setNodeStatus(NodeStatus.ACTIVE);
                    }
                }
                //System.out.println(" -- After reading an Application Message -- " +  clientChannel.getRemoteAddresses() + " clock " + getVectorClock().toString());

            } catch (Exception e) {
                throw  new RuntimeException(e);
            }
        }

        private void updateVectorClock(ArrayList<Integer> mClock) {
            for (int i = 0; i < mClock.size(); i++) {
                int val = Math.max(getVectorClock().get(i), mClock.get(i));
                getVectorClock().set(i, val);
            }
            getVectorClock().set(getNodeNum(), getVectorClock().get(getNodeNum()) + 1);
        }

        private void closeEverything() {
            if (getClientChannel() != null && clientChannel.isOpen()) {
                try {
                    //System.out.println(" -- Closing a client channel -- " + clientChannel.getRemoteAddresses());
                    if( getClientChannel().isOpen()) {
                        getClientChannel().close();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (getListenerChannel() != null && getListenerChannel().isOpen()) {
                try {
                    //System.out.println(" -- Closing Listener Channel -- ");
                    getListenerChannel().close();
                } catch ( Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

}