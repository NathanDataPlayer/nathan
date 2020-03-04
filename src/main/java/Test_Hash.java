import java.util.Random;

public class Test_Hash {




    int numPartitions =3;
    private int partitiondefine(String key){
        if (key == null){
            return  new Random().nextInt(numPartitions);
        }else {
            return Math.abs(key.hashCode()%numPartitions);
        }
    }

    public static void main(String[] args) {
        Test_Hash tsh = new Test_Hash();
        int nums = tsh.partitiondefine("report_report_no");
        int nums1 = tsh.partitiondefine("loss_loss_no");
        System.out.println(nums);
        System.out.println(nums1);


        String dir ="hdfs://ns1/user/hive/warehouse/dual";

        String[] a =dir.split("/");

        String tes = a[a.length-1];

        System.out.println(tes);



    }
}