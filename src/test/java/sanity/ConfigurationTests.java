package sanity;

import com.evolveum.polygon.connector.grouper.GrouperConfiguration;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.testng.Assert;
import org.testng.annotations.Test;
import util.CommonTestClass;

import java.util.Arrays;

import static org.testng.AssertJUnit.assertEquals;

public class ConfigurationTests extends CommonTestClass {
    private static final Log LOG = Log.getLog(ConfigurationTests.class);

    @Test(expectedExceptions = ConfigurationException.class)
    public void initializeNotCorrectConfigurationNullValues() {

        grouperConfiguration.setHost(null);
        grouperConfiguration.setPort(null);
        grouperConfiguration.validate();

        Assert.fail();
    }

    @Test
    public void testGetSetTheProperties() {
        GrouperConfiguration testConfiguration = new GrouperConfiguration();

        testConfiguration.setHost("127.0.0.1");
        assertEquals("127.0.0.1", testConfiguration.getHost());
        testConfiguration.setPort("5432");
        assertEquals("5432", testConfiguration.getPort());
        testConfiguration.setUserName("midpoint");
        assertEquals("midpoint", testConfiguration.getUserName());
        testConfiguration.setDatabaseName("grouper");
        assertEquals("grouper", testConfiguration.getDatabaseName());
        testConfiguration.setConnectionValidTimeout(20);
        assertEquals(Integer.valueOf(20), testConfiguration.getConnectionValidTimeout());

        String[] extendedGroupPropertiesn = new String[]{"something", "something1", "something2"};


        testConfiguration.setExtendedGroupProperties(new String[]{"something", "something1", "something2"});
        Assert.assertTrue(Arrays.equals(testConfiguration.getExtendedGroupProperties(), extendedGroupPropertiesn));

        String[] extendedSubjectPropertiesn = new String[]{"23456789", "12345678", "98764543", "A12345678"};
        testConfiguration.setExtendedSubjectProperties(new String[]{"23456789", "12345678", "98764543", "A12345678"});
        Assert.assertTrue(Arrays.equals(testConfiguration.getExtendedSubjectProperties(), extendedSubjectPropertiesn));

        testConfiguration.setExcludeDeletedObjects(false);
        assertEquals(Boolean.FALSE, testConfiguration.getExcludeDeletedObjects());
    }

    @Test
    public void testDefaultValues() {
        GrouperConfiguration testConfiguration = new GrouperConfiguration();

        assertEquals(Integer.valueOf(10), testConfiguration.getConnectionValidTimeout());
        assertEquals(Boolean.TRUE, testConfiguration.getExcludeDeletedObjects());
    }
}
