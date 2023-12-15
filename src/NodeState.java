import java.io.Serializable;
import java.util.ArrayList;

public class NodeState implements Serializable {
    int nodeNum;
    ArrayList<Integer> clock;
    NodeStatus status;

    public NodeState(int nodeNum, ArrayList<Integer> clock, NodeStatus status) {
        this.nodeNum = nodeNum;
        this.clock = clock;
        this.status = status;
    }

    public int getNodeNum() {
        return nodeNum;
    }

    public void setNodeNum(int nodeNum) {
        this.nodeNum = nodeNum;
    }

    public ArrayList<Integer> getClock() {
        return clock;
    }

    public void setClock(ArrayList<Integer> clock) {
        this.clock = clock;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public void setStatus(NodeStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return " node num : " + getNodeNum() + " status :  " + getStatus() + " clock  : " + getClock().toString();
    }
}
