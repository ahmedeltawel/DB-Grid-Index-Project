
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

public class GridIndex implements Serializable {
    public Hashtable<String, Integer> dimensions = new Hashtable<String, Integer>();
    Hashtable<Integer, Vector<Comparable[]>> ranges = new Hashtable<Integer, Vector<Comparable[]>>();
    Hashtable<Integer, String> minString = new Hashtable<Integer, String>();
    Table parentTable = null;
    CArray grid;
    public Vector<Column> columns;

    // Create Empty Index
    public GridIndex(Vector<Column> columns, Table tableCreatedOn) {
        this.columns = columns;
        this.parentTable = tableCreatedOn;

        int[] dim = new int[columns.size()];

        int i = 0;
        for (Column col : columns) {
            dim[i] = 10; // each dim in CArray has 10 values

            dimensions.put(col.name, i);

            Vector<Comparable[]> minMax = new Vector<Comparable[]>();

            minMax.add(new Comparable[] {});
            ranges.put(i, divideToRanges(col));
            i++;

        }

        grid = new CArray(dim);
    }

    public GridCell getCellFromRow(Row row) {
        // gets the cell where the row should be put in
        // usefull to check if row can be indexd in this index or not

        // Check that every indexed dim is in the row
        Vector<String> dimNames = new Vector<String>();
        Enumeration dimNameEnum = dimensions.keys();
        while (dimNameEnum.hasMoreElements()) {
            String dimName = (String) dimNameEnum.nextElement();
            if (!row.containsKey(dimName)) {
                try {
                    throw new Exception("Row Must contain all indexed values");
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                    e.printStackTrace();
                }

            } else
                dimNames.add(dimName);
        }

        // Name = ahmed
        // age = 16
        // salry = 3333

        /**
         * <Column | operator | value > < Name = ahemed> < Age = 55 >
         */

        Vector<Vector> terms = new Vector<Vector>();
        for (String dimName : dimNames) {
            Vector v = new Vector<>();
            v.add(dimName);
            v.add("=");
            v.add(row.get(dimName)); // value
            terms.add(v);
        }

        // [0,1,2,3,4,5,6,7,8,9] ranges
        // 0 dim name <3>
        // 1 dim age <2>
        // 2 dime salry <5>

        Hashtable<Integer, Vector<Integer>> matchingRangesPerDim = getMatchingRangesPerDim(terms);

        int[] coord = new int[dimensions.size()];

        // must match every single dimetion in the index

        for (Integer dim : matchingRangesPerDim.keySet()) {
            Vector<Integer> diee = matchingRangesPerDim.get(dim);
            coord[dim] = matchingRangesPerDim.get(dim).get(0);
        }
        // CArray coordinate [0: 3, 1: 2, 2: 5]

        List<Integer> coordList = new ArrayList<Integer>(coord.length);
        for (int i : coord)
            coordList.add(i);

        GridCell cell = grid.getAt(coordList);
        if (cell != null)
            return cell;
        else {
            // if(cell does not exist in the CAarry create it)
            GridCell newCell = new GridCell();
            grid.addAt(newCell, coordList);
            return newCell;
        }
    }

    public void addRow(Row row, Address savedAddrInfo) {
        GridCell cell = getCellFromRow(row);
        cell.addRow(savedAddrInfo);
    }

    public void removeRow(Row removedRow, Object primaryKey) {
        GridCell cell = getCellFromRow(removedRow);
        cell.removeRow(primaryKey);
    }

    public void updateRow(Row oldRow, Row newRow, Address newAddress) {
        removeRow(oldRow, newAddress.pkValue);
        addRow(newRow, newAddress);
    }

    private Vector<Comparable[]> divideToRanges(Column col) {
        Vector<Comparable[]> ranges = new Vector();

        if (col.MinValue instanceof java.util.Date) {
            Date startDate = (Date) col.MinValue;
            Date endDate = (Date) col.MaxValue;

            long diffInMillies = Math.abs(endDate.getTime() - startDate.getTime());
            long diff = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);

            diff = diff / 10;

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(startDate);

            int i = 0;
            while (i < 10) {
                i++;
                Comparable[] minmax = new Comparable[2];
                minmax[0] = calendar.getTime();

                if (i != 9)
                    calendar.add(Calendar.DAY_OF_MONTH, -1);

                calendar.add(Calendar.DAY_OF_MONTH, (int) diff);

                minmax[1] = calendar.getTime();

                if (i == 10)
                    minmax[1] = endDate;

                calendar.add(Calendar.DAY_OF_MONTH, +1);

                ranges.add(minmax);
            }
        }

        if (col.MinValue instanceof java.lang.Integer) {
            Integer min = (Integer) col.MinValue;
            Integer max = (Integer) col.MaxValue;

            int diff = max - min;
            diff = diff / 10;

            Integer mid = min;

            int i = 0;
            while (i < 10) {
                i++;
                Comparable[] minmax = new Comparable[2];
                minmax[0] = mid;

                if (i != 9)
                    mid -= 1;

                mid += diff;

                if (i == 10)
                    mid = max;

                minmax[1] = mid;

                mid += 1;
                ranges.add(minmax);
            }

        }

        if (col.MinValue instanceof java.lang.Double) {
            Double min = (Double) col.MinValue;
            Double max = (Double) col.MaxValue;

            double diff = max - min;
            diff = diff / 10.0;

            Double mid = min;

            int i = 0;
            while (i < 10) {
                i++;
                Comparable[] minmax = new Comparable[2];
                minmax[0] = mid;

                if (i != 9) {
                    String sValue = (String) String.format("%.6f", mid - 0.00001);
                    mid = Double.parseDouble(sValue);
                    // mid -= 0.00001;
                }

                mid += diff;
                if (i == 10)
                    mid = max;

                String sValue = (String) String.format("%.6f", mid);
                mid = Double.parseDouble(sValue);

                minmax[1] = mid;
                // mid += 0.00001;

                sValue = (String) String.format("%.6f", mid + 0.00001);
                mid = Double.parseDouble(sValue);

                ranges.add(minmax);
            }

        }

        if (col.MinValue instanceof java.lang.String) {
            String min = (String) col.MinValue;
            String max = (String) col.MaxValue;

            long range = stringRanges.countRange(min, max);
            long diff = range / 10;

            long mid = 0;

            int i = 0;
            while (i < 10) {
                i++;
                Comparable[] minmax = new Comparable[2];
                minmax[0] = mid;
                if (i != 9)
                    mid -= 1;
                mid += diff;

                if (i == 10)
                    mid = range;

                minmax[1] = mid;

                mid += 1;

                ranges.add(minmax);
            }
            int dimNum = dimensions.get(col.name);
            minString.put(dimNum, min);
        }

        return ranges;
    }

    public Vector<Row> queryIndex(Query inputQuery) {
        Vector<GridCell> gridCells = evaluateIndexQuery(inputQuery);

        Vector<Row> resultRows = new Vector<Row>();
        for (GridCell gridCell : gridCells) {
            for (String bucketPath : gridCell.bucketList) {
                Bucket bucket = gridCell.loadBucket(bucketPath);
                for (Address address : bucket.addresses) {
                    resultRows.add(address.grapRow(this.parentTable));
                }
            }
        }
        return resultRows;
    }

    public Vector<GridCell> evaluateIndexQuery(Query inputQuery) {
        int index = 0;
        String operation = "";

        if (inputQuery.operators.contains("XOR")) {
            index = inputQuery.operators.indexOf("XOR");
            operation = "XOR";
        } else if (inputQuery.operators.contains("OR")) {
            index = inputQuery.operators.indexOf("OR");
            operation = "OR";
        } else {
            // some Is anded Tohether
            Vector<Vector<Integer>> buffer = new Vector<Vector<Integer>>();
            for (String dim : dimensions.keySet())
                buffer.add(new Vector<Integer>());

            for (Object elment : inputQuery.terms) {
                SQLTerm term = (SQLTerm) elment;
                int dimNumber = dimensions.get(term._strColumnName);
                Vector<Integer> matchingRanges = getMatchingRanges(term);
                Vector<Integer> alreadyInBuffer = buffer.get(dimNumber);
                Vector<Integer> resultInDim = (alreadyInBuffer.size() == 0) ? matchingRanges : rangesAnd(alreadyInBuffer, matchingRanges);
                buffer.set(dimNumber, resultInDim);
            }
            // fix matched, vary the rest of dimentions
            Integer[] startCoord = new Integer[dimensions.size()];
            Integer[] endCoord = new Integer[dimensions.size()];

            for (int i = 0; i < dimensions.size(); i++) {
                startCoord[i] = 0;
                if (!buffer.get(i).isEmpty()) {
                    startCoord[i] = buffer.get(i).firstElement();
                    endCoord[i] = buffer.get(i).lastElement();
                } else
                    endCoord[i] = 9;
            }

            Vector<GridCell> resultGrids = grid.SelectRange(Arrays.asList(startCoord), Arrays.asList(endCoord));
            return resultGrids;
        }

        Query leftQuery = Query.subQuery(inputQuery, 0, index);
        Query rightQuery = Query.subQuery(inputQuery, index + 1, inputQuery.size());
        Vector<GridCell> leftResult = evaluateIndexQuery(leftQuery);
        Vector<GridCell> rightResult = evaluateIndexQuery(rightQuery);
        return performLogicOperationVectors(leftResult, rightResult, operation);
    }

    private Vector performLogicOperationVectors(Vector leftVector, Vector rightVector, String operator) {
        switch (operator) {
            case "AND":
                return rangesAnd(leftVector, rightVector);
            case "OR":
                return rangesOr(leftVector, rightVector);
            case "XOR":
                return rangesXor(leftVector, rightVector);
        }
        return null;
    }

    private Vector rangesAnd(Vector bufferDimentionValues, Vector matchingRanges) {
        Vector toBeRemoved = new Vector<>();
        for (Object element : matchingRanges)
            if (!bufferDimentionValues.contains(element))
                toBeRemoved.add(element);

        matchingRanges.removeAll(toBeRemoved);
        return matchingRanges;
    }

    private Vector rangesOr(Vector bufferDimentionValues, Vector matchingRanges) {
        for (Object intRangeValue : matchingRanges)
            if (!bufferDimentionValues.contains(intRangeValue))
                bufferDimentionValues.add(intRangeValue);

        return bufferDimentionValues;
    }

    private Vector rangesXor(Vector bufferDimentionValues, Vector matchingRanges) {
        Vector result = new Vector();
        for (Object intRangeValue : matchingRanges)
            if (!bufferDimentionValues.contains(intRangeValue))
                result.add(intRangeValue);
            else
                bufferDimentionValues.remove(intRangeValue);

        result.addAll(bufferDimentionValues);
        return result;
    }

    public Hashtable<Integer, Vector<Integer>> getMatchingRangesPerDim(Vector<Vector> terms) {
        Hashtable<Integer, Vector<Integer>> matchingRangesPerDim = new Hashtable<Integer, Vector<Integer>>();

        for (Vector term : terms) {
            String column = (String) term.get(0);
            int dim = dimensions.get(column);
            matchingRangesPerDim.put(dim, getMatchingRanges(term));
        }
        return matchingRangesPerDim;
    }

    public Vector<Integer> getMatchingRanges(SQLTerm term) {
        String column = (String) term._strColumnName;
        String cond = (String) term._strOperator;
        Comparable value = (Comparable) term._objValue;
        return getMatchingRanges(column, cond, value);
    }

    public Vector<Integer> getMatchingRanges(Vector term) {
        String column = (String) term.get(0);
        String cond = (String) term.get(1);
        Comparable value = (Comparable) term.get(2);
        return getMatchingRanges(column, cond, value);
    }

    public Vector<Integer> getMatchingRanges(String column, String op, Comparable value) {
        Vector<Integer> matchingRanges = new Vector<Integer>();

        int dim = dimensions.get(column);
        Vector<Comparable[]> columnRanges = ranges.get(dim);

        // handling String range searches, the 10th position in ranges will hold min as
        // String
        if (value instanceof String) {
            String minValue = minString.get(dim);
            value = (Comparable) stringRanges.countRange(minValue, (String) value);
        }

        for (int i = 0; i < columnRanges.size(); i++) {
            Comparable[] minMax = columnRanges.get(i);
            switch (op) {
                case "=":
                    if (value.compareTo(minMax[0]) >= 0 && value.compareTo(minMax[1]) <= 0)
                        matchingRanges.add(i);
                    break;
                case ">":
                    if (value.compareTo(minMax[1]) < 0)
                        matchingRanges.add(i);
                    break;
                case "<":
                    if (value.compareTo(minMax[0]) > 0)
                        matchingRanges.add(i);
                    break;
                case ">=":
                    if (value.compareTo(minMax[1]) <= 0)
                        matchingRanges.add(i);
                    break;
                case "<=":
                    if (value.compareTo(minMax[0]) >= 0)
                        matchingRanges.add(i);
                    break;
            }
        }
        return matchingRanges;
    }

    public static void main(String[] args) throws ParseException {
        Vector<Integer> ranges = new Vector<Integer>();
        Vector<Integer> buffer = new Vector<Integer>();
        ranges.add(3);
        ranges.add(2);
        ranges.add(5);

        // buffer.add(5);

        Column c = new Column("nfriheu", "rogiehwg");
        c.MinValue = 0;
        c.MaxValue = 19;
        Vector<Column> cols = new Vector<Column>();
        cols.add(c);
        // GridIndex g = new GridIndex(cols);

        // System.out.println(g.rangesAnd(buffer, ranges));

    }

}
