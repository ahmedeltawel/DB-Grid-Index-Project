

import java.util.Vector;


import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class CArray extends Vector {
    int[] dimensions;

    public CArray(int[] dimensions) {
        super();
        this.dimensions = dimensions;
        this.addAll(recurseInitDim(new Vector<Vector>(), dimensions, 0));
    }

    private Vector recurseInitDim(Vector<Vector> arr, int[] dimensions, int level) {
        if (dimensions.length > level) {
            for (int i = 0; i < dimensions[level]; i++) {
                arr.add(new Vector<>());
            }
            if (dimensions.length - 1 == level)
                return arr;
        } else {
            return arr;
        }
        level++;
        for (int j = 0; j < arr.size(); j++) {
            arr.set(j, recurseInitDim(new Vector<>(), dimensions, level));
        }
        return arr;
    }

    public void addAt(GridCell obj, List coordinate) {
        Vector<Integer> coord = new Vector<Integer>(coordinate);

        RemoveRecurse(this, obj, coord);
    }

    private void RemoveRecurse(Vector arr, Object obj, Vector coordinate) {
        if (coordinate.isEmpty()) { // reached !!
            arr.add(obj);
        } else {
            arr = (Vector) arr.get((int) coordinate.get(0));
            coordinate.remove(0);
            RemoveRecurse(arr, obj, coordinate);
        }
    }

    public GridCell getAt(List coordinate) {
        Vector<Integer> coord = new Vector<Integer>(coordinate);
        return convertToGrid(getRecurse(this, coord));
    }

    private Vector getRecurse(Vector arr, Vector coordinate) {
        if (coordinate.isEmpty()) { // reached !!
            return arr;
        } else {
            arr = (Vector) arr.get((int) coordinate.get(0));
            coordinate.remove(0);
            return getRecurse(arr, coordinate);
        }
    }

    public GridCell removeAt(List coordinate) {
        Vector<Integer> coord = new Vector<Integer>(coordinate);

        return convertToGrid(RemoveRecurse(this, coord));
    }

    private Vector<GridCell> RemoveRecurse(Vector arr, Vector coordinate) {
        if (coordinate.isEmpty()) { // reached !!
            Vector old = (Vector) arr.clone();
            arr.clear();
            return old;
        } else {
            arr = (Vector) arr.get((int) coordinate.get(0));
            coordinate.remove(0);
            return RemoveRecurse(arr, coordinate);
        }
    }

    public Vector<GridCell> SelectRange(List<Integer> R1, List<Integer> R2) {
        Vector<Integer> R1C = new Vector<Integer>(R1);
        Vector<Integer> R2C = new Vector<Integer>(R2);
        Vector rslt = SelectRecurse(this, R1C, R2C);
        Vector Final = new Vector<>();
        for (int i = 0; i < rslt.size(); i++) {
            Final.addAll((Collection) rslt.get(i));
        }
        return Final;
    }

    private Vector SelectRecurse(Vector arr, Vector<Integer> R1, Vector<Integer> R2) {
        Vector Result = new Vector<>();
        Vector trimresult = new Vector<>();
        for (int i = R1.get(0); i <= R2.get(0); i++) {
            Vector l = (Vector) arr.get(i);
            if (!(l).isEmpty())
                trimresult.add(l);
        }

        R1.remove(0);
        R2.remove(0);
        if (R1.isEmpty())
            return trimresult;
        for (int i = 0; i < trimresult.size(); i++) {
            Vector rtn = SelectRecurse((Vector) trimresult.get(i), (Vector<Integer>) R1.clone(),
                    (Vector<Integer>) R2.clone());
            if (!rtn.isEmpty())
                Result.addAll(rtn);
        }

        return Result;
    }

    public Vector<GridCell> selectOrRanges(List<Integer> R1, List<Integer> R2) {
        /** by convetion -1 means don't care about that dimetion
        r1: [2,5,-1]
        r2: [7,6,-1]
        ============
        r1: [2,0,0]
        r2: [7,9,9]
            OR
        r1: [0,5,0]
        r2: [9,6,9]
            OR
        r1: [0,5,0]
        r2: [9,6,9]
         */
        Vector<Integer> R1C = new Vector<Integer>(R1);
        Vector<Integer> R2C = new Vector<Integer>(R2);

        Integer[] startCoord = new Integer[dimensions.length];
        Integer[] endCoord = new Integer[dimensions.length];

        Vector<Integer> indexOfOred = new Vector<Integer>();
        Vector<GridCell> result = new Vector<GridCell>();
        HashSet<GridCell> uniqueCells = new HashSet<GridCell>();

        for (int i = 0; i < R1C.size(); i++) {
            int num = R1C.get(i);
            if (num != -1)
                indexOfOred.add(i);
        }

        for (Integer index : indexOfOred) {
            for (int i = 0; i < dimensions.length; i++) {
                startCoord[i] = 0;
                endCoord[i] = dimensions[i] - 1;
            }
            startCoord[index] = R1C.get(index);
            endCoord[index] = R2C.get(index);

            result.addAll( SelectRange(Arrays.asList(startCoord), Arrays.asList(endCoord)) );
            uniqueCells.addAll(result);
        }

        for (GridCell gridCell : uniqueCells)
            result.add(gridCell);

        return result;
    }
    public Vector<GridCell> selectXOrRanges(List<Integer> R1, List<Integer> R2) {
        // XOR = OR - AND
        Vector<GridCell> Or = new Vector<GridCell>();
        Or = selectOrRanges(R1, R2);

        Vector<GridCell> And = new Vector<GridCell>();
        And = selectOrRanges(R1, R2);
        
        Or.removeAll(And);

        return Or;
    }

    private GridCell convertToGrid(Vector v) {
        GridCell g;
        try {
            g = (GridCell) v.get(0);
        } catch (Exception e) {
            return null;
        }
        return g;
    }

    public static void main(String[] args) {
        // int[] dim = { 3, 3 };
        // CArray arr = new CArray(dim);
        // arr.addAt(3, Arrays.asList(0, 0));
        // arr.addAt(5, Arrays.asList(1, 0));
        // arr.addAt(7, Arrays.asList(0, 1));
        // arr.addAt(9, Arrays.asList(1, 1));

        // System.out.println(arr.SelectRange(Arrays.asList(0, 0), Arrays.asList(1,
        // 1)));

        Vector<Integer> coord = new Vector<Integer>(Arrays.asList(1, 2, 3));
        System.out.println(coord.get(0));

    }

}
