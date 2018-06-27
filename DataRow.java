public class DataRow {
    private final int[] intData = new int[2];

    public DataRow(int a, int b) {
        intData[0] = a;
        intData[1] = b;
    }

    public DataRow(String a, String b) {
        intData[0] = Integer.valueOf(a);
        intData[1] = Integer.valueOf(b);
    }

    public int[] getData() {
        return intData;
    }

    @Override
    public String toString() {
        return "Row: " + intData[0] + intData[1];
    }
}