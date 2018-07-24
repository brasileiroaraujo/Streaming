package DataStructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MetablockingNodeCollection implements Serializable{
	private static final long serialVersionUID = 466473482117035453L;
	
	List<MetablockingNode> nodeList = new ArrayList<MetablockingNode>();

	public List<MetablockingNode> getNodeList() {
		return nodeList;
	}

	public void setNodeList(List<MetablockingNode> nodeList) {
		this.nodeList = nodeList;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	public void add(MetablockingNode node) {
		this.nodeList.add(node);
	}

	public void blackList() {
		nodeList = new ArrayList<MetablockingNode>();
		MetablockingNode blackNode = new MetablockingNode(-1, false);
		blackNode.setBlackFlag(true);
		nodeList.add(blackNode);
	}
	
	public boolean isOnTheBlackList() {
		if (!nodeList.isEmpty()) {
//			System.out.println();
			if (nodeList.get(0).isBlackFlag()) {
				return true;
			}
		}
			
		return false;
	}

	public void removeOldNodes(int timeThreshold) {
		long currentTime = System.currentTimeMillis();
		ArrayList<MetablockingNode> copy = new ArrayList<MetablockingNode>(nodeList);
		for (MetablockingNode node : copy) {
			if ((currentTime - node.getStartTime()) > (timeThreshold*1000)) {//*1000 to convert in miliseconds
				nodeList.remove(node);
			}
		}
		copy = null;
	}
	
	

}
