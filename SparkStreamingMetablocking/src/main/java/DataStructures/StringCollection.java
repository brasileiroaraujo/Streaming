package DataStructures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class StringCollection implements Serializable{

	private static final long serialVersionUID = -451481391613026333L;
	
	List<String> nodeList = new ArrayList<String>();

	public List<String> getNodeList() {
		return nodeList;
	}

	public void setNodeList(List<String> nodeList) {
		this.nodeList = nodeList;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}
	
	public void add(String node) {
		this.nodeList.add(node);
	}
	
	public void setAsMarked(String label) {
		for (int i = 0; i < nodeList.size(); i++) {
			if (nodeList.get(i).split(";").length == 3) {
				nodeList.set(i, nodeList.get(i) + ";" + label);
			} else {
				nodeList.set(i, nodeList.get(i).substring(0, nodeList.get(i).length()-2) + ";" + label);
			}
			
		}
	}

}
