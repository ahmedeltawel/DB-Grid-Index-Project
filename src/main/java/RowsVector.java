

import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

/**
 * RowVector
 */
public class RowsVector extends Vector<Row>  {
    PageActionListener actionListener;

    @Override
    public synchronized void insertElementAt(Row row, int index) {
        try {
            addRowActions(row);
            super.insertElementAt(row, index);
        } catch (Exception e) {
        }
    }

    public boolean add(Hashtable<String, Object> ht) {
        // Automatically convert added hashtables to row type and add
        Row newRow = new Row();
        newRow.putAll(ht);
        return add(newRow);
    }

    @Override
    public synchronized boolean add(Row row) {
        boolean success = false;

        try {
            addRowActions(row);
            success = super.add(row);
        } catch (Exception e) {
        }
        return success;
    }

    @Override
    public synchronized Row remove(int index) {
        Row oldRow = (Row) this.get(index).clone();
        Row row = super.remove(index);
        actionListener.updateIndexOnRowRemove(oldRow, row);
        
        removeRowActions(row);
        return row;
    }

    @Override
    public synchronized void removeAllElements() {
        actionListener.updatePageMinMaxOnClear();
        super.removeAllElements();
    }

    /**
     * TODO: other methods removeAt remove(int xx)
     * 
     * add( 1, 2, 3) insertElementAt()
     */

    private void removeRowActions(Row row) {
        try {
            actionListener.RowRemoved();
            actionListener.updatePageMinMaxRemoveRow(row);
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void addRowActions(Row row) throws Exception {
        try {
            actionListener.verifyAddRow(row);
            // The listener of the row is the vectors' listener the(Page)
            row.setActionListener(actionListener);

            actionListener.updatePageMinMaxAddRow(row);

        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
            throw new Exception();
        }
    }

    public void setActionListener(PageActionListener listener) {
        this.actionListener = listener;
    }

}