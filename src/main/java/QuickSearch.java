/*
快速查找并且节约空间
 */

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

public class QuickSearch {
    public static void main(String[] args) {
        Random random = new Random();

        List<Integer> list = new ArrayList<>();
        //产生10个随机数
        for(int i=0;i<10;i++){
            int randomResult=random.nextInt(10);
            list.add(randomResult);
        }

        System.out.println("产生的随机数-------");
        for (int i=0;i<list.size();i++){
            System.out.println(list.get(i));
        }

        BitSet bitset = new BitSet(100);
        for (int i=0;i<10;i++) {
            bitset.set(list.get(i));
        }

        ///
        for (int i=0;i <100; i++){
            if (!bitset.get(i)) {
                System.out.println("不在100里面的有"+i);
            }
        }
    }
}
