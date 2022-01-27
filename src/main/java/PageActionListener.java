

public interface PageActionListener {
    public void verifyAddRow(Row row) throws Exception;
    public void verifyValidRow(Row row) throws Exception;
    public void updatePageMinMaxAddRow(Row row);
    public void updatePageMinMaxRemoveRow(Row row);
    public void updatePageMinMaxOnClear();
    public void RowRemoved();
    public void updateIndexOnRowRemove(Row oldRow, Row newRow);
}
