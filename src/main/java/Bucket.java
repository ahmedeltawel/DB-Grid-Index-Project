

import java.io.Serializable;
import java.util.Vector;

/**
 *
    *  Read DBApp.config and verify max size
    *  store the location information in BucketEntrys vector
    *  
 */
public class Bucket implements Serializable{
   int bucketId = 0;
   Vector<Address> addresses ;
   GridCell parentCell;
   int maxEntriesInBucket;
   

   public Bucket(int bucketId, GridCell parentCell){
      this.bucketId = bucketId;
      this.addresses = new Vector<>();
      this.parentCell = parentCell;
      maxEntriesInBucket = helper.readConfig("MaximumRowsCountinPage");
   }
    
   public void addAddress(Address address){
      if(addresses.size() > maxEntriesInBucket){
         parentCell.bucketOverFlow(address);
         return;
      }
      addresses.add(address);
   }
   public void removeAddress(Address address){
      if(addresses.size() == 0){
         parentCell.bucketUnderFlow(this);
         return;
      }
      addresses.remove(address);
   }



}