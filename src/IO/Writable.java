package IO;

import java.io.DataInput;
import java.io.DataOutput;

public interface Writable {
  void write (DataOutput out);
  void readFields(DataInput in);
  

}
