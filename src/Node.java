import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class Node {
    Integer numOfNodes;
    Integer interRequestDelay;
    Integer csExecutionTime;
    Integer numOfRequests;
    String systemName;
    Integer nodeNum;
    String nodeHost;
    Integer clock;
    HashMap<Integer,Boolean> keyMap = new HashMap<>();

    public Integer getClock() {
        return clock;
    }

    public void setClock(Integer clock) {
        this.clock = clock;
    }

    public HashMap<Integer, Boolean> getKeyMap() {
        return keyMap;
    }

    public void setKeyMap(HashMap<Integer, Boolean> keyMap) {
        this.keyMap = keyMap;
    }

    private HashMap<Integer, String> nodeMap = new HashMap<Integer, String>();

    private HashMap<Integer, Integer> portMap = new HashMap<Integer, Integer>();

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

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public Integer getNumOfNodes() {
        return numOfNodes;
    }

    public void setNumOfNodes(Integer numOfNodes) {
        this.numOfNodes = numOfNodes;
    }

    public Integer getInterRequestDelay() {
        return interRequestDelay;
    }

    public void setInterRequestDelay(Integer interRequestDelay) {
        this.interRequestDelay = interRequestDelay;
    }

    public Integer getCsExecutionTime() {
        return csExecutionTime;
    }

    public void setCsExecutionTime(Integer csExecutionTime) {
        this.csExecutionTime = csExecutionTime;
    }

    public Integer getNumOfRequests() {
        return numOfRequests;
    }

    public void setNumOfRequests(Integer numOfRequests) {
        this.numOfRequests = numOfRequests;
    }

    @Override
    public String toString() {
        return "Node{" +
                "numOfNodes=" + numOfNodes +
                ", interRequestDelay=" + interRequestDelay +
                ", csExecutionTime=" + csExecutionTime +
                ", numOfRequests=" + numOfRequests +
                ", systemName='" + systemName + '\'' +
                ", nodeNum=" + nodeNum +
                ", nodeHost='" + nodeHost + '\'' +
                ", nodeMap=" + nodeMap +
                ", portMap=" + portMap +
                '}';
    }

    public static void main(String[] args) {
        String str = String.join(" ", args);
        System.out.println(str);
        String[] input = str.split("-");
        for(String s : input) {
            System.out.println(s);
        }
        Node node = new Node();
        try {
            node.setSystemName(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        for (int i = 0; i < input.length; i++) {
            String[] temp = input[i].split(" ");
            if (i == 0) {
                node.setNumOfNodes(Integer.parseInt(temp[0]));
                node.setInterRequestDelay(Integer.parseInt(temp[1]));
                node.setCsExecutionTime(Integer.parseInt(temp[2]));
                node.setNumOfRequests(Integer.parseInt(temp[3]));

            } else {
                node.getNodeMap().put(Integer.parseInt(temp[0]), temp[1]+".utdallas.edu");
                if ((node.getSystemName()).contains(temp[1])) {
                    node.setNodeHost(temp[1]);
                    node.setNodeNum(Integer.parseInt(temp[0]));
                }
                node.getPortMap().put(Integer.parseInt(temp[0]), Integer.parseInt(temp[2]));
            }
        }
        for(int i=0;i< node.getNumOfNodes();i++) {
            if(i>= node.getNodeNum()) {
                node.getKeyMap().put(i,true);
            } else {
                node.getKeyMap().put(i,false);
            }
        }
        System.out.println(node.getNodeNum());
        System.out.println(node.getKeyMap());
        Service service = new Service(node.getKeyMap(),node.getClock(), node.getNodeNum(), node.getPortMap(), node.getNodeMap());
        new Thread() {
            @Override
            public void run() {
                service.createListener();
            }
        };
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        service.createRequesters();
    }
}
