/**
 * Created by shaowei on 6/21/17.
 */
public class Test {
    public static void main(String[] args){
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<10; i++){
            sb.append(i+"haha");
            sb.append(",");
        }

        sb.append("end");
        sb.deleteCharAt(sb.length()-1);

        System.out.println(sb.toString());
    }

}
