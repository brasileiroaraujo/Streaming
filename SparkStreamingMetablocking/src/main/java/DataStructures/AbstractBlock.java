package DataStructures;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author G.A.P. II
 */

public abstract class AbstractBlock implements Serializable {

    private static final long serialVersionUID = 7526443743449L;

    protected double comparisons;
    protected int blockIndex;
    protected double utilityMeasure;

    protected double entropy;

    public AbstractBlock() {
        blockIndex = -1;
        utilityMeasure = -1;
    }

    public AbstractBlock(double entropy) {
        blockIndex = -1;
        utilityMeasure = -1;
        this.entropy = entropy;
    }

    public int getBlockIndex() {
        return blockIndex;
    }

    public ComparisonIterator getComparisonIterator() {
        return new ComparisonIterator(this);
    }

    public double getNoOfComparisons() {
        return comparisons;
    }

    public double getUtilityMeasure() {
        return utilityMeasure;
    }

    public double processBlock(AbstractDuplicatePropagation adp) {
        double noOfComparisons = 0;

        ComparisonIterator iterator = getComparisonIterator();
        while (iterator.hasNext()) {
            Comparison comparison = iterator.next();
            if (!adp.isSuperfluous(comparison)) {
                noOfComparisons++;
            }
        }

        return noOfComparisons;
    }

    public double getEntropy() {
        return entropy;
    }

    public void setBlockIndex(int blockIndex) {
        this.blockIndex = blockIndex;
    }

    public List<Comparison> getComparisons() {
        final List<Comparison> comparisons = new ArrayList<>();

        ComparisonIterator iterator = getComparisonIterator();
        while (iterator.hasNext()) {
            Comparison comparison = iterator.next();
            comparisons.add(comparison);
        }

        return comparisons;
    }

    public abstract double getTotalBlockAssignments();
    public abstract double getAggregateCardinality();
    public abstract void setUtilityMeasure();

	public String showBlock() {
		// TODO Auto-generated method stub
		return null;
	}
}
