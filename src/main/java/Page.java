

import java.io.Serializable;
import java.util.Enumeration;

import org.sk.PrettyTable;


/**
 * Page Class
 **/

public class Page implements Serializable, PageActionListener {
    transient Table parentTable;
    public int number = 0;
    public RowsVector Rows; // Vector Of (Row) datatype
    public int maxRowsPerPage;
    public Comparable minPkeyValue;
    public Comparable maxPkeyValue;
    
    
    // TODO: Min Max to do Binary search, null value problem when seraching

    public Page(int pageNumber, Table parentTable) {
        this.number = pageNumber;
        this.parentTable = parentTable;
        Rows = new RowsVector();
        Rows.setActionListener(this);

        maxRowsPerPage = helper.readConfig("MaximumRowsCountinPage");
    }

    // Action Listener
    @Override
    public void verifyValidRow(Row obj) throws Exception {
        // CHECK :: Rows is of Type Row
        Row row;
        try {
            row = (Row) obj;
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("Row must be of type Row");
        }
        if(row.isEmpty()) throw new Exception("Row must not be Empty");

        
        enumColOfInsertedRow:
        for (String colName : row.keySet()) {
            Comparable value = (Comparable) row.get(colName);

            for (Column parentColumn : parentTable.Columns) {
                if (colName.equals(parentColumn.name)) {
                    // CHECK :: Rows is of same types as each coloumn
                    String valueType = value.getClass().getName().toLowerCase();

                    if (!(valueType.equals(parentColumn.strType)))
                        throw new Exception(String.format("Cant add value %s in the column %s only accepts %s",
                                value.toString(), parentColumn.name, parentColumn.strType));

                    int c1 = helper.compareObjects(value, parentColumn.MinValue);
                    int c2 = helper.compareObjects(value, parentColumn.MaxValue);
                    if (c1 < 0 || c2 > 0) { // less than the minimum, or more than the maximum
                        throw new Exception(String.format("Min Max Coloumn Contstraint Violation, Row update rejected! Value %s outside of [%s <=> %s]",value.toString(), parentColumn.MinValue.toString(), parentColumn.MaxValue.toString()));  
                    }
                    continue enumColOfInsertedRow;
                }
            }
            // If execution reached here then it didn't match with the table columns
            throw new Exception("The inserted row has a Column Mismatch");
        }
    }

    @Override
    public void verifyAddRow(Row row) throws Exception {
        if (Rows.size() >= maxRowsPerPage)
            throw new Exception(String.format("Cant add row to page %s, it excedes maximum rows per page", number));

        verifyValidRow(row);
    }

    @Override
    public void updatePageMinMaxAddRow(Row row) {
        String pkColName = parentTable.getPrimaryColumn().name;
        Comparable<Object> addedPKvalue = (Comparable) row.get(pkColName);

        if (minPkeyValue == null || maxPkeyValue == null) { // if yet doesn't have values
            minPkeyValue = addedPKvalue;
            maxPkeyValue = addedPKvalue;
            return;
        }

        int c1 = helper.compareObjects(addedPKvalue, minPkeyValue);
        int c2 = helper.compareObjects(addedPKvalue, maxPkeyValue);

        if (c1 < 0) { // less than the minimum
            minPkeyValue = addedPKvalue;
        }
        if (c2 > 0) { // more than the minimum
            maxPkeyValue = addedPKvalue;
        }
    }

    @Override
    public void updatePageMinMaxRemoveRow(Row row) {
        String pkColName = parentTable.getPrimaryColumn().name;
        Comparable<Object> removedPKvalue = (Comparable) row.get(pkColName);

        if (Rows.size() == 0) { // if yet doesn't have values
            minPkeyValue = null;
            maxPkeyValue = null;
            return;
        }
        int c1 = helper.compareObjects(removedPKvalue, minPkeyValue);
        int c2 = helper.compareObjects(removedPKvalue, maxPkeyValue);

        if (c1 <= 0) { // less than the minimum
            // the new minimum should be row(0)
            minPkeyValue =(Comparable) this.Rows.get(0).get(pkColName);
        }
        if (c2 >= 0) { // more than the maximum
            //the new maximum should be row.size()-1
            maxPkeyValue = (Comparable) this.Rows.get(this.Rows.size()-1).get(pkColName);
        }


    }

    @Override
    public void RowRemoved() {
        if (Rows.size() == 0 && this.number != 0) {
            parentTable.deletePage(this.number);
        }
        
    }
    @Override
    public void updatePageMinMaxOnClear() {
        this.minPkeyValue = null;
        this.maxPkeyValue = null;

        RowRemoved();
    }
    // #region
    @Override
    public String toString() {
        if(parentTable == null) return "";
        int i = 0;
        String headers[];
        Enumeration colsEnum = parentTable.Columns.elements();
        int numberOfColumns = parentTable.Columns.size();
        headers = new String[numberOfColumns];
        while (colsEnum.hasMoreElements()) {
            Column c = (Column) colsEnum.nextElement();
            headers[i] = (c.isPK?"*"+c.name:c.name);
            i++;
        }

        PrettyTable ptable = new PrettyTable(headers);

        // WARN: the row hashtable order might not be aligned with the columns
        Enumeration rowsEnum = Rows.elements();
        while (rowsEnum.hasMoreElements()) { // For each ROW
            Row row = (Row) rowsEnum.nextElement();

            String[] pTableRowValues = new String[parentTable.Columns.size()];
            i = 0;
            colsEnum = parentTable.Columns.elements();
            while (colsEnum.hasMoreElements()) {
                Column c = (Column) colsEnum.nextElement();
                String colName = c.name;
                Object value = row.get(colName);
                pTableRowValues[i] = (value == null ? "" : value.toString());
                i++;
            }
            ;

            ptable.addRow(pTableRowValues);
        }

        return (ptable.toString());
    }
    public boolean isFull(){
        if(Rows.size() < maxRowsPerPage) return false;
        return true;
    }
    // #endregion

    public static void main(String[] args) {
        
    }

    @Override
    public void updateIndexOnRowRemove(Row oldRow, Row newRow) {
       parentTable.indiciesUpdateRow(oldRow, newRow, new Address(parentTable.TableName, newRow.get(parentTable.getPrimaryColumn().name), this.number)); 

    }

   

}
