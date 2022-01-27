

import java.io.File;
import java.io.Serializable;
import java.util.Vector;

public class GridCell implements Serializable{
    Vector<String> bucketList;
    static String buckets_directory = "src/main/resources/data/";

    public GridCell() {
        bucketList = new Vector<String>();

    }

    public Bucket addNewBucket() {
        int i = 0;
        String bucketPath;
        do {
            bucketPath = formatBucketPath(i);
            i++;
        } while ((new File(bucketPath)).exists());

        Bucket newBucket = new Bucket(--i, this);
        helper.serialize(newBucket, bucketPath);
        bucketList.add(bucketPath);

        return newBucket;
    }

    public void removeBucket(int bucketId) {
        String bucketPath = formatBucketPath(bucketId);
        bucketList.remove(bucketPath);
        try {
            helper.delFile(bucketPath);
        } catch (Exception e) {
            System.out.println(String.format("Couldn't delbuk %s", bucketId, bucketPath));
        }
    }

    public void removeBucket(Bucket b) {
        removeBucket(b.bucketId);
    }

    // Listener of addAddress inside Bucket
    void bucketOverFlow(Address address) {
        addNewBucket().addAddress(address);
    }

    void bucketUnderFlow(Bucket b) {
        removeBucket(b);
    }

    public void addRow(Address addr) {
        if(bucketList.size() == 0)
            addNewBucket();
        
        // load last bucket add in it
        String bucketPath = bucketList.lastElement();
        Bucket b = loadBucket(bucketPath);
        b.addAddress(addr);
        saveBucket(b, bucketPath);
    }

    public void removeRow(Object primaryKey) {
        for (String bucketPath : bucketList) {
            Bucket b = loadBucket(bucketPath);
            for (Address address : b.addresses) {
                if(address.equals(primaryKey)){
                    b.addresses.remove(address);
                    return;
                }
            }
        }
    }
   
    public Bucket loadBucket(String bucketPath) {
        try {
            return (Bucket) helper.deserialize(bucketPath);
        } catch (Exception e) {
            System.out.println(String.format("Failed to deserialize bucket %s", bucketPath));
            e.printStackTrace();
        }
        return null;
    }
    
    public void saveBucket(Bucket bucjetObj,  String bucketPath) {
        try {
            helper.serialize(bucjetObj, bucketPath);
        } catch (Exception e) {
            System.out.println(String.format("Failed to serialise bucket %s", bucketPath));
            e.printStackTrace();
        }
    }

    public String formatBucketPath(int bucketId) {
        return String.format("%sBucket_%s", buckets_directory, bucketId);
    }

    

   

}
