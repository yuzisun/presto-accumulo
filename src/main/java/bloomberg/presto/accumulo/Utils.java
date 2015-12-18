package bloomberg.presto.accumulo;

import java.util.Arrays;

public class Utils {

    /**
     * Given two app engine key names, returns the key name that comes between
     * them.
     * 
     * @param a_kn
     * @param b_kn
     * @return
     */
    public static byte[] average(byte[] a_kn, byte[] b_kn) {

        int[] a = new int[a_kn.length + 1];
        int[] b = new int[b_kn.length + 1];
        for (int i = 0; i < a_kn.length; ++i) {
            a[i + 1] = (int) a_kn[i];
        }

        for (int i = 0; i < b_kn.length; ++i) {
            b[i + 1] = (int) b_kn[i];
        }

        int[] avg = ldiv2(ladd(a, b));
        byte[] tmp = new byte[avg.length - 1];
        for (int i = 1; i < avg.length; ++i) {
            tmp[i - 1] = (byte) avg[i];
        }

        return tmp;
    }

    /**
     * Given a list of digits after the decimal point, returns a new list of
     * digits representing that number divided by two.
     * 
     * @param vals
     * @return
     */
    private static int[] ldiv2(int[] vals) {
        int base = 128;

        int[] vs = Arrays.copyOf(vals, vals.length + 1);
        int i = vs.length;

        while (i > 0) {
            i -= 1;
            if ((vs[i] % 2) == 1) {
                vs[i] -= 1;
                vs[i + 1] += base / 2;
            }
            vs[i] = vs[i] / 2;
        }

        if (vs[vs.length - 1] == 0) {
            vs = Arrays.copyOf(vs, vs.length - 1);
        }

        return vs;
    }

    /**
     * Given two lists representing a number with one digit left to decimal
     * point and the rest after it, for example 1.555 = [1,5,5,5] and 0.235 =
     * [0,2,3,5], returns a similar list representing those two numbers added
     * together.
     * 
     * @param a
     * @param b
     * @return
     */
    private static int[] ladd(int[] a, int[] b) {
        int base = 128;
        int i = Math.max(a.length, b.length);
        int[] lsum = new int[i];
        while (i > 1) {
            i -= 1;
            int av, bv;
            av = bv = 0;
            if (i < a.length) {
                av = a[i];
            }

            if (i < b.length) {
                bv = b[i];
            }

            lsum[i] += av + bv;

            if (lsum[i] >= base) {
                lsum[i] -= base;
                lsum[i - 1] += 1;
            }
        }

        return lsum;
    }
}
