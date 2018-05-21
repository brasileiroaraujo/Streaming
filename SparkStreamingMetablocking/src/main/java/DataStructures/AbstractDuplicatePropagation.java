package DataStructures;

import DataStructures.Comparison;
import DataStructures.IdDuplicates;

import java.util.Set;

/**
 * @author gap2
 */

public abstract class AbstractDuplicatePropagation {

    private final String name;
    protected final int existingDuplicates;
    protected final Set<IdDuplicates> duplicates;

    public AbstractDuplicatePropagation(Set<IdDuplicates> matches) {
        duplicates = matches;
        existingDuplicates = duplicates.size();
        name = "Duplicate Propagation";
    }

    public AbstractDuplicatePropagation(String groundTruthPath) {
        duplicates = (Set<IdDuplicates>) SerializationUtilities.loadSerializedObject(groundTruthPath);
        existingDuplicates = duplicates.size();
        name = "Duplicate Propagation";
    }

    public int getExistingDuplicates() {
        return existingDuplicates;
    }

    public String getName() {
        return name;
    }

    public abstract int getNoOfDuplicates();
    public abstract boolean isSuperfluous(Comparison comparison);
    public abstract void resetDuplicates();

    public Set<IdDuplicates> getDuplicates() {
        return duplicates;
    }
}
