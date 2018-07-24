package DataStructures;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import scala.Tuple2;

public class MetablockingNode implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -9010084079861024226L;
	private Integer entityId;
	private Set<Integer> blocks;
	private boolean blackFlag = false;
	private long startTime;
	private boolean marked;
	private boolean isSource;
	private Set<Tuple2<Integer, Double>> neighbors;
	private double sumWeight = 0.0;
	private int numberOfNeighbors = 0;
	
	
	public MetablockingNode(Integer id, boolean isSource) {
		super();
		this.entityId = id;
		this.blocks = new HashSet<Integer>();
		this.neighbors = new HashSet<Tuple2<Integer, Double>>();
		this.startTime = System.currentTimeMillis();
		this.isSource = isSource;
	}


	public Integer getEntityId() {
		return entityId;
	}


	public void setEntityId(Integer entityId) {
		this.entityId = entityId;
	}


	public Set<Integer> getBlocks() {
		return blocks;
	}


	public void setBlocks(Set<Integer> blocks) {
		this.blocks = blocks;
	}
	
	public void addInBlocks(Integer block) {
		this.blocks.add(block);
	}


	public boolean isBlackFlag() {
		return blackFlag;
	}


	public void setBlackFlag(boolean blackFlag) {
		this.blackFlag = blackFlag;
	}


	public long getStartTime() {
		return startTime;
	}


	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}


	public boolean isMarked() {
		return marked;
	}


	public void setMarked(boolean marked) {
		this.marked = marked;
	}


	public boolean isSource() {
		return isSource;
	}


	public void setSource(boolean isSource) {
		this.isSource = isSource;
	}
	
	public Set<Tuple2<Integer, Double>> getNeighbors() {
		return neighbors;
	}


	public void setNeighbors(Set<Tuple2<Integer, Double>> neighbors) {
		this.neighbors = neighbors;
	}
	
	public void addNeighbor(Tuple2<Integer, Double> neighbor) {
		this.sumWeight += neighbor._2();
		this.numberOfNeighbors ++;
		this.neighbors.add(neighbor);
	}


	public double getSumWeight() {
		return sumWeight;
	}


	public int getNumberOfNeighbors() {
		return numberOfNeighbors;
	}
	
	public void addSetNeighbors(MetablockingNode n2) {
		this.sumWeight += n2.getSumWeight();
		this.numberOfNeighbors += n2.getNumberOfNeighbors();
		this.neighbors.addAll(n2.getNeighbors());
	}
	
	public void pruning() {
		double threshold = sumWeight/numberOfNeighbors;
		Set<Tuple2<Integer, Double>> prunnedNeighbors = new HashSet<>();
		
		for (Tuple2<Integer, Double> neighbor : neighbors) {
			if (neighbor._2() >= threshold) {
				prunnedNeighbors.add(neighbor);
			}
		}
		
		this.neighbors = prunnedNeighbors;
		
	}
	
	
}
