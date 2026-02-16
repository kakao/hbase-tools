package com.kakao.hbase.stat.print;

import org.junit.Assert;
import org.junit.Test;

public class ColorTest {
    @Test
    public void testColor() throws Exception {
        for (Color color : Color.values()) {
            System.out.println(color.build(color.name(), Formatter.Type.ANSI));
        }
    }

    @Test
    public void testLength() throws Exception {
        String stringWithoutColor = "string";
        String stringCyan = Color.cyan.build(stringWithoutColor, Formatter.Type.ANSI);
        String stringBold = Color.bold.build(stringWithoutColor, Formatter.Type.ANSI);

        Assert.assertEquals(stringWithoutColor, Color.clearColor(stringWithoutColor, Formatter.Type.ANSI));
        Assert.assertEquals(stringWithoutColor, Color.clearColor(stringCyan, Formatter.Type.ANSI));
        Assert.assertEquals(stringWithoutColor, Color.clearColor(stringBold, Formatter.Type.ANSI));
        Assert.assertEquals(stringWithoutColor.length(), Color.lengthWithoutColor(stringWithoutColor, Formatter.Type.ANSI));
        Assert.assertEquals(stringWithoutColor.length(), Color.lengthWithoutColor(stringCyan, Formatter.Type.ANSI));
        Assert.assertEquals(stringWithoutColor.length(), Color.lengthWithoutColor(stringBold, Formatter.Type.ANSI));
    }
}
