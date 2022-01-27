
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

public class Table implements Serializable {
    private static final long serialVersionUID = 6529685098267757690L;

    public Vector<GridIndex> indices;
    public Vector<Column> Columns;
    public transient Vector<Page> PagesBuffer;
    public Hashtable<Integer, String> PagesPath;

    public String TableName;
    static String pages_directory = "src/main/resources/data/";
    static String TableInfo_directory = "src/main/resources/data/";

    public Table(String strTableName, String strPk, Hashtable<String, String> htbColNameType,
            Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax) {
        this.TableName = strTableName;

        Columns = new Vector<Column>();
        PagesBuffer = new Vector<Page>();
        PagesPath = new Hashtable<Integer, String>();
        indices = new Vector<GridIndex>();

        addPageLast();

        // Load store the given Column Data
        htbColNameType.forEach((colName, type) -> {
            try {
                Column c = new Column(colName, type);
                if (colName.equals(strPk))
                    c.isPK = true;
                Columns.add(c);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Load and Store Min Max data
        setTableColumnsMinMaxUsing(htblColNameMin, 0);
        setTableColumnsMinMaxUsing(htblColNameMax, 1);
        helper.addTableToMetaDataFile(this);
    }

    private void setTableColumnsMinMaxUsing(Hashtable<String, String> htbl, Integer isMax) {
        // isMax to defrentiate between them Min or Max
        try {
            colValuesParsing: for (String colName : htbl.keySet()) {
                String value = htbl.get(colName);
                for (Column col : Columns) {
                    if (col.name.equals(colName)) {
                        Comparable parsedValue = (Comparable) helper.parseStringButRespectType(value, col.strType);
                        if (isMax == 0)
                            col.MinValue = parsedValue;
                        else
                            col.MaxValue = parsedValue;
                        continue colValuesParsing;
                    }
                }
                // when givin min max does not match tables
                throw new Exception("The givin min max does not match table's");
            }
        } catch (Exception e) {
            System.out.println("Couldn't Parse givin Column Min/Max Values " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static Table loadTable(String TableName) {
        String path = TableInfo_directory + TableName;
        Table loadedTable = null;
        try {
            loadedTable = (Table) helper.deserialize(path);
            loadedTable.PagesBuffer = new Vector<Page>();
        } catch (Exception e) {
        }

        return loadedTable;
    }

    public Page addPageFirst() {
        int newPageId = -1;
        Page newPage = new Page(newPageId, this);
        PagesBuffer.add(newPage);
        PagesPath.put(newPageId, formatedPagePath(newPage.number));

        savePage(newPageId);
        saveTableInfo();
        PagesBuffer.remove(newPage);

        renameThePages();

        try {
            loadPage(0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return newPage;
    }

    // public Page addPageMiddle(Integer newPageID) {
    // int newId = newPageID * -1;
    // Page newPage = new Page(newId, this);
    // PagesBuffer.add(newPage);
    // PagesPath.put(newId, formatedPagePath(newPage.number));

    // savePage(newId);
    // saveTableInfo();
    // PagesBuffer.remove(newPage);

    // renameThePages();

    // try {
    // loadPage(newPage.number);
    // } catch (Exception e2) {
    // e2.printStackTrace();
    // }
    // return newPage;
    // }

    public Page addoverflow(Integer newPageID) {
        // input 2
        Enumeration<Integer> enumeration = PagesPath.keys();
        while (enumeration.hasMoreElements()) {
            Integer key = enumeration.nextElement();
            if (key >= newPageID) {
                String old = PagesPath.remove(key);
                int newkey = key + 1;
                int last = old.lastIndexOf("_");
                String n = old.substring(0, last + 1) + newkey;
                // String n = old.substring(0, old.length()-1) + newkey;
                PagesPath.put(newkey, n);
                try {
                    helper.renameFile(old, n);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            helper.delFile(formatedPagePath(newPageID));
        } catch (Exception e) {
            // System.out.println(String.format("Tried to Delete Page %s, from disk but ist
            // there", newPageID));
        }

        saveTableInfo();

        // int newId = newPageID * -1;

        int newPageId = newPageID;
        Page newPage = new Page(newPageId, this);
        PagesBuffer.clear();
        PagesBuffer.add(newPage);
        PagesPath.put(newPageId, formatedPagePath(newPage.number));

        savePage(newPageId);
        saveTableInfo();
        return newPage;

    }

    public Page addPageLast() {
        int newPageId = getPagesCount();
        Page newPage = new Page(newPageId, this);
        PagesBuffer.add(newPage);
        PagesPath.put(newPageId, formatedPagePath(newPage.number));

        savePage(newPageId);
        saveTableInfo();
        return newPage;
    }

    public void deletePage(int pageNumber) {
        Page p = getBufferPageById(pageNumber);

        try {
            PagesPath.remove(pageNumber);
            PagesBuffer.remove(p);
        } catch (Exception e) {
            // System.out.println(String.format("Tried to Delete Page %s, but was not in
            // Buffer", pageNumber));
        }

        try {
            helper.delFile(formatedPagePath(pageNumber));
        } catch (Exception e) {
            System.out.println(String.format("Tried to Delete Page %s, from disk but ist there", pageNumber));
        }

        renameThePages();

        saveTableInfo();
    }

    public Page loadPage(int pageNumber) throws Exception {
        String path = PagesPath.get(pageNumber);
        // CHECK page stored in pathes, and not already loaded in the buffer
        if (path != null) {
            Page alreadyloaded = getBufferPageById(pageNumber);
            if (alreadyloaded != null)
                return alreadyloaded;
            
            Page loadedPage = (Page) helper.deserialize(path);
            PagesBuffer.add(loadedPage);
            loadedPage.parentTable = this;
            loadedPage.Rows.actionListener = loadedPage;

            // CHECK if page number does not match location page numbering
            if (loadedPage.number != pageNumber)
                loadedPage.number = pageNumber;
            return loadedPage;
        } else
            throw new Exception("Couldn't Find the Page in pageDictionary");

    }

    public void unloadPage(Page PageRef) throws Exception {
        String path = PagesPath.get(PageRef.number);
        helper.serialize(PageRef, path);
        PagesBuffer.remove(PageRef);
    }

    public void unloadPage(int pageNumber) throws Exception {
        // Save the unloaded page
        Page unloadedPage = getBufferPageById(pageNumber);
        if (unloadedPage == null)
            throw new Exception("Unloading a Page that is not loaded");
        unloadPage(unloadedPage);
    }

    public void unloadAllBuffer() throws Exception {
        Vector<Page> bufferClone = (Vector<Page>) PagesBuffer.clone();
        for (Page p : bufferClone)
            unloadPage(p);
    }

    public void saveTable() {
        saveAllPages();
        saveTableInfo();
    }

    private void saveAllPages() {
        for (Page p : PagesBuffer) {
            String path = PagesPath.get(p.number);
            helper.serialize(p, path);
        }
    }

    private void saveTableInfo() {
        String path = TableInfo_directory + TableName;
        helper.serialize(this, path);
    }

    public void savePage(int pNumber) {
        for (Page p : PagesBuffer) {
            if (p.number == pNumber) {
                String path = PagesPath.get(p.number);
                helper.serialize(p, path);
            }
        }

    }

    public Integer BinarySearchForRow(Page page, Object searchValue) {
        String pkColName = getPrimaryColumn().name;
        int pageSize = page.Rows.size();
        if (pageSize == 0)
            return null;
        int hi = pageSize;
        int lo = 0;
        int cmp = -1;
        while (lo < hi) {
            int mid = Math.floorDiv(hi + lo, 2); // Math.floorDiv(hi - lo, 2) + lo;
            Row row = page.Rows.get(mid);
            cmp = helper.compareObjects(row.get(pkColName), searchValue);
            if (cmp > 0) // Row pK > search value
                hi = mid; // left
            else
                lo = mid + 1; // right
        }
        lo -= 1;
        try {
            if (page.Rows.get(lo).get(pkColName).equals(searchValue))
                return lo;
        } catch (Exception e) {
        }
        return null;
    }

    public Integer BinarySearchForRowInsert(Page page, Object searchValue) {
        String pkColName = getPrimaryColumn().name; // [2,3,5,7,10] 1
        int pageSize = page.Rows.size() - 1;
        int hi = pageSize;
        int lo = 0;
        int cmp = -1;
        int cmp0 = -1;
        cmp0 = helper.compareObjects(page.minPkeyValue, searchValue);
        if (cmp0 > 0)
            return 0;

        cmp0 = helper.compareObjects(page.maxPkeyValue, searchValue);
        if (cmp0 < 0)
            return page.Rows.size();

        if (page.Rows.size() < 3) {
            if (page.Rows.size() == 1) {
                return 1;
            } else {
                if (page.Rows.size() == 2) {
                    cmp0 = helper.compareObjects(page.maxPkeyValue, searchValue);
                    if (cmp0 > 0)
                        return 1;
                    else
                        return 2;
                } else {
                    return 0;
                }
            }
        } else {
            while (lo < hi) { // 0<5
                int mid = Math.floorDiv(hi + lo, 2); // Math.floorDiv(hi - lo, 2) + lo; // mid=2
                Row row = page.Rows.get(mid);
                Row rowMin = page.Rows.get(mid - 1);
                Row rowMax = page.Rows.get(mid + 1);
                cmp = helper.compareObjects(row.get(pkColName), searchValue);
                if (cmp > 0) {// Row pK < search value
                    cmp = helper.compareObjects(rowMin.get(pkColName), searchValue);
                    if (cmp < 0) {
                        return mid;
                    }
                    hi = mid; // left
                } else {
                    cmp = helper.compareObjects(rowMax.get(pkColName), searchValue);
                    if (cmp > 0) {
                        return mid + 1;
                    }
                    lo = mid + 1; // right
                }
            }
            if (lo == 0)
                return null;
            lo -= 1;
            if (!page.Rows.get(lo).get(pkColName).equals(searchValue))
                return null;

            return lo;
        }
    }

    public Page BinarySearchForPage(Object searchValue) {
        // WARN: will load the Page; you should unload when not needed!
        // WARN: If needed, Store the minmax page info in the table, make table action
        // listens for adding/rem of rows
        // listener, listen to rowsvectors add(row) evnet -> update minmax
        // ArrayList<Integer> pageNumbersList = new ArrayList<>(PagesPath.keySet()); //
        // this to not make unsafe load to out of order page
        // Collections.sort(pageNumbersList);
        // WARN: May not be necessary if all pages are ordered sequentially with no gaps
        // page = loadPage(pageNumbersList.get(mid));

        // BUG: if there's an empty page, the binary search will fail, b/c of min max
        // are nulls

        return BinarySearchRecurse(0, PagesPath.size(), searchValue);
    }

    private Page BinarySearchRecurse(int lo, int hi, Object searchValue) {
        Page page = null;
        try {
            while (lo <= hi) {
                int mid = Math.floorDiv(hi + lo, 2); // mid = 1
                page = loadPage(mid);
                if (page != null) {
                    if (page.minPkeyValue != null) {
                        int cmpMn = helper.compareObjects(page.minPkeyValue, searchValue);
                        if (cmpMn > 0) { // search less than min
                            if (lo == hi)
                                return null;
                            hi = mid; // go left
                        } else {
                            int cmpMx = helper.compareObjects(page.maxPkeyValue, searchValue);
                            if (cmpMx < 0) { // search more than max
                                if (lo == hi)
                                    return null;
                                lo = mid + 1; // go right
                            } else // between min and max
                                return page;
                        }
                        unloadPage(page);
                    } else {
                        if (lo == hi)
                            return null;

                        Page rightResult = BinarySearchRecurse(mid + 1, hi, searchValue);
                        if (rightResult == null) {
                            Page leftResult = BinarySearchRecurse(lo, mid, searchValue);
                            return leftResult;
                        } else
                            return rightResult;
                    }
                }
            }
            lo -= 1;
            return loadPage(lo);
        } catch (Exception e) {
        }
        return null;
    }

    public Integer getTotalRowCount() throws Exception {
        Integer ctr = 0;
        Enumeration enumPathes = PagesPath.keys();
        while (enumPathes.hasMoreElements()) {
            Integer pageNumbder = (Integer) enumPathes.nextElement();
            Page p = loadPage(pageNumbder);
            ctr += p.Rows.size();
            unloadPage(p);
        }
        ;
        return ctr;
    }

    public Page BinarySearchForPageInsert(Object searchValue) {
        // // 0 1 2
        // // [null] [4,20] [24,24] [20,29]

        // String pkColName = getPrimaryColumn().name;

        // int pageSize = page.Rows.size();
        // if (pageSize == 0)
        // return null;
        // int hi = pageSize;
        // int lo = 0;
        // int cmp = -1;
        // while (lo < hi) {
        // int mid = Math.floorDiv(hi + lo, 2); // Math.floorDiv(hi - lo, 2) + lo;
        // Row row = page.Rows.get(mid);
        // cmp = helper.compareObjects(row.get(pkColName), searchValue);
        // if (cmp > 0) // Row pK > search value
        // hi = mid; // left
        // else
        // lo = mid + 1; // right
        // }
        // lo -= 1;
        // try {
        // if (page.Rows.get(lo).get(pkColName).equals(searchValue))
        // return lo;
        // } catch (Exception e) {
        // }

        return BinarySearchRecurseInsert(0, PagesPath.size() - 1, searchValue);
        // return null;
    }

    private Page BinarySearchRecurseInsert(int lo, int hi, Object searchValue) {
        // System.out.println(hi); 0, 3 (0,{1},2) 20
        // [null] [4,20] [24,24] 3
        Page page = null;
        Page page2 = null;
        try {
            while (lo < hi) { // 0 < 1
                int mid = Math.floorDiv(hi + lo, 2); // mid = 0
                page = loadPage(mid);
                int cmp0 = helper.compareObjects(page.minPkeyValue, searchValue);
                if (mid == 0 && cmp0 > 0) {
                    return page;
                }

                if (this.PagesPath.get(mid + 1) == null) {
                    // System.out.println("elmafrod null");
                    return page;
                } else {
                    page2 = loadPage(mid + 1);
                }
                if (page != null) {
                    if (page.minPkeyValue != null) {
                        int cmpMn = helper.compareObjects(page.minPkeyValue, searchValue);
                        if (cmpMn > 0) { // search less than min
                            hi = mid; // go left high =0
                        } else {
                            int cmpMx = helper.compareObjects(page.maxPkeyValue, searchValue);
                            if (cmpMx < 0) { // search more than max
                                int cmpMn2 = helper.compareObjects(page2.minPkeyValue, searchValue);
                                if (cmpMn2 > 0) {
                                    return page;
                                }
                                lo = mid + 1; // go right =2
                            } else { // between min and max
                                return page;
                            }
                        }
                        unloadPage(page);
                        unloadPage(page2);
                    } else {
                        Page rightResult = BinarySearchRecurseInsert(mid + 1, hi, searchValue);
                        if (rightResult == null) {
                            Page leftResult = BinarySearchRecurseInsert(lo, mid, searchValue);
                            return leftResult;
                        } else
                            return rightResult;
                    }
                }
            }
            if (lo == 0 || lo == hi) {
                page = loadPage(PagesPath.size() - 1);
                return page;
            }
            return loadPage(lo); // 2
        } catch (Exception e) {
            System.out.println("Error (loading/unloading pages, or Page has no MinMax) while searching");
            e.printStackTrace();
        }
        return null;
    }

    // #region
    public int getPagesCount() {
        return PagesPath.size();
    }

    private String formatedPagePath(int pageNumber) {
        return String.format("%s%s_Page_%s", pages_directory, TableName, pageNumber);
    }

    private Page getBufferPageById(int pageNumber) {
        for (Page p : PagesBuffer)
            if (p.number == pageNumber)
                return p;
        return null;
    }

    private void renameThePages() {
        /**
         * By convention if page number is -1, this means add it to be the first page if
         * page number is -X other than 1, this means add page named X at location X
         */

        Vector<Integer> keysOrdered = new Vector<Integer>();
        PagesPath.forEach((key, v) -> {
            keysOrdered.add(key);
        });
        // set Page if needed to be inserted in the middle
        Integer pageToInsertMiddle = 0;
        for (Integer key : keysOrdered)
            if (key < -1) {
                pageToInsertMiddle = -1 * key;
                break;
            }

        Collections.sort(keysOrdered);
        Collections.reverse(keysOrdered);

        Enumeration<Integer> pagePathNumbers = keysOrdered.elements();

        int i = PagesPath.size() - 1;
        while (pagePathNumbers.hasMoreElements()) {
            int pNumber = (Integer) pagePathNumbers.nextElement(); // next Key
            String path = PagesPath.get(pNumber);

            if (pNumber != i && PagesPath.get(i) == null) { // order using i, and number not used before

                String newPageName = formatedPagePath(i);

                if (pageToInsertMiddle != 0 && (pageToInsertMiddle) == i) {
                    newPageName = formatedPagePath(pageToInsertMiddle);
                }
                try {
                    helper.renameFile(path, newPageName);
                } catch (IOException e) {
                    System.out.println(String.format("Failed to Rename the page %s to %s", formatedPagePath(pNumber),
                            newPageName));
                    e.printStackTrace();
                }
                PagesPath.remove(pNumber); // remove old path
                PagesPath.put(i, newPageName); // add new path
            }
            i--;
        }

    }

    public Column getPrimaryColumn() {
        for (Column c : Columns)
            if (c.isPK)
                return c;
        return null;
    }

    // #endregion
    @Override
    public String toString() {
        String output = "";
        Vector<Integer> keysOrdered = new Vector<Integer>();
        for (Integer pNumber : PagesPath.keySet()) {
            keysOrdered.add(pNumber);
        }
        Collections.sort(keysOrdered);

        try {
            for (Integer pNumber : keysOrdered) {
                Page page = loadPage(pNumber);
                if (page != null) {
                    output += "Page_" + page.number + "\n" + page.toString();
                    unloadPage(page);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return output;
    }

    public void indiciesAddRow(Row row, Address savedAddrInfo) {
        Vector<GridIndex> indicies = allIndiciesMatching(row);
        if (indicies != null)
            for (GridIndex index : indicies) 
                index.addRow(row, savedAddrInfo);
    }

    public void indiciesRemoveRow(Row removedRow, Object primaryKey) {
        Vector<GridIndex> indicies = allIndiciesMatching(removedRow);
        if (indicies != null)
            for (GridIndex index : indicies) 
                index.removeRow(removedRow, primaryKey);
    }

    public void indiciesUpdateRow(Row oldRow, Row newRow, Address newAddress) {
        Vector<GridIndex> indicies = allIndiciesMatching(oldRow);
        if (indicies != null)
            for (GridIndex index : indicies) 
                index.updateRow(oldRow, newRow, newAddress);
    }

    public GridIndex searchingIndex(Query query) {
        Vector<String> colNames = new Vector<String>();
        for (Object term : query.terms) 
            if(term instanceof SQLTerm)
                colNames.add(((SQLTerm)term)._strColumnName);

        return searchingIndex(colNames);
    }
    public GridIndex searchingIndex(Set<String> stringSet) {
        Vector<String> colNames = new Vector<String>();
        for (String colName : stringSet) 
            colNames.add(colName);
        return searchingIndex(colNames);
    }
    public GridIndex searchingIndex(Hashtable<String, Object>  ht) {
        Vector<String> colNames = new Vector<String>();
        for (String colName : ht.keySet()) 
            colNames.add(colName);
        return searchingIndex(colNames);
    }

    public Vector<GridIndex> allIndiciesMatching( Set<String> stringSet ) {
        Vector<String> colNames = new Vector<String>();
        for (String colName : stringSet) 
            colNames.add(colName);
        return allIndiciesMatching(colNames);
    }
    public Vector<GridIndex> allIndiciesMatching( Hashtable<String, Object>  ht ) {
        Vector<String> colNames = new Vector<String>();
        for (String colName : ht.keySet()) 
            colNames.add(colName);
        return allIndiciesMatching(colNames);
    }
    public GridIndex searchingIndex(Vector<String> columnNames) {
        // gets the index with the lowist other indexing columns other than the given
        GridIndex result = null;
        Vector<GridIndex> avIndicies = allIndiciesMatching(columnNames);
        int lowistOtherColumnsCount = Integer.MAX_VALUE;
        for (GridIndex index : avIndicies) {
            int columnsOtherThanIndexing = 0;
            for (Column column : index.columns) {
                if (!columnNames.contains(column.name))
                    columnsOtherThanIndexing++;
            }
            if (columnsOtherThanIndexing < lowistOtherColumnsCount)
                result = index;
        }
        return result;
    }

    public Vector<GridIndex> allIndiciesMatching(Vector<String> columnNames) {
        // All indicies matching the givin column Names, usefull when updating all the
        // indicies

        Vector<GridIndex> result = new Vector<GridIndex>();
        for (GridIndex index : indices)
            for (Column column : index.columns)
                if (columnNames.contains(column.name))
                    if(!result.contains(index))
                        result.add(index);
        return result;
    }

    public static void main(String[] args) throws Exception {

        // Hashtable<String,String> htblColNameType = new Hashtable<String, String>();
        // htblColNameType.put("id", "java.lang.Integer");
        // htblColNameType.put("name", "java.lang.String");
        // htblColNameType.put("gpa", "java.lang.double");

        // Table table = new Table("Students", "id", htblColNameType, new
        // Hashtable<String,String>(), new Hashtable<String,String>());

        // Hashtable<String, Object> data = new Hashtable<String, Object>();
        // data.put("id", 1);
        // data.put("name", "ahmed");
        // data.put("gpa", 1.2);
        // Hashtable<String, Object> data2 = new Hashtable<String, Object>();
        // data2.put("id", 2);
        // data2.put("name", "Khaled");
        // data2.put("gpa", 2.2);
        // Hashtable<String, Object> data3 = new Hashtable<String, Object>();
        // data3.put("id", 3);
        // data3.put("name", "lo2ay");
        // data3.put("gpa", 3.2);
        // table.PagesBuffer.get(0).Rows.add(data);
        // // System.out.println(table.PagesBuffer.get(0).Rows);
        // table.PagesBuffer.get(0).Rows.add(data2);
        // // System.out.println(table.PagesBuffer.get(0).Rows);
        // table.PagesBuffer.get(0).Rows.add(data3);

        // table.saveAllPages();

        // System.out.println(table.PagesBuffer.get(0).Rows);

    }

}
