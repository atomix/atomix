package io.atomix.utils.misc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class StringUtilsTest {

    @Test
    public void testNull() {
        assertNull(StringUtils.split(null, ","));
    }

    @Test
    public void testFilter() {
        String[] result = StringUtils.split("1,  ,,", ",");
        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals("1", result[0]);
    }
}