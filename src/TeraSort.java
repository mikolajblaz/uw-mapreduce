import java.util.Arrays;

class HelloWorld {
    public static void main(String[] args) {
        System.out.println("Hello");
        map();
    }

    static void map() {
        int[] a = {1, 2, 3, 4};
        int[] b = new int[4];
        for (int i = 0; i < 4; i++) {
            b[i] = a[i] + 1;
        }
        for (int i = 0; i < 4; i++) {
            System.out.println(b[i]);
        }
    }
}