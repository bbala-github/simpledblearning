package simpledb;

import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClientBuilder;
import com.amazonaws.services.simpledb.model.*;

import java.util.ArrayList;
import java.util.Arrays;

public class SimpleDbLearning {
    private static String domainName = "persons";

    private static void createDomain(AmazonSimpleDB sdb) {
        try {
            CreateDomainResult cdres = sdb.createDomain(new CreateDomainRequest(domainName));
            System.out.println("createDomain succeeded");
        } catch (Exception e) {
            System.out.println("createDomain failed, err=" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void deleteDomain(AmazonSimpleDB sdb) {
        try {
            sdb.deleteDomain(new DeleteDomainRequest(domainName));
            System.out.println("deleteDomain succeeded");
        } catch (Exception e) {
            System.out.println("deleteDomain failed, err=" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void listDomains(AmazonSimpleDB sdb) {
        try {
            ListDomainsResult ldres = sdb.listDomains();
            System.out.println("List of domains:");
            for (String s : ldres.getDomainNames()) System.out.println(s);
        } catch (Exception e) {
            System.out.println("listDomains failed, err=" + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void put(AmazonSimpleDB sdb) {
        ReplaceableAttribute ra1 = new ReplaceableAttribute("age", "34", false);
        ReplaceableAttribute ra2 = new ReplaceableAttribute("age", "32", false);
        PutAttributesRequest pareq1 = new PutAttributesRequest(domainName, "bala", new ArrayList<ReplaceableAttribute>(Arrays.asList(ra1)));
        PutAttributesRequest pareq2 = new PutAttributesRequest(domainName, "siva", new ArrayList<ReplaceableAttribute>(Arrays.asList(ra2)));
        try {
            sdb.putAttributes(pareq1);
            sdb.putAttributes(pareq2);
            System.out.println("put succeeded");
        } catch (Exception e) {
            System.out.println("put failed, err=" + e.getMessage());
        }
    }

    private static void dumpAttributes(GetAttributesResult gares) {
        System.out.println("Attributes:");
        for (Attribute a : gares.getAttributes()) {
            System.out.println(a.getName() + " -> " + a.getValue());
        }
    }

    private static void get(AmazonSimpleDB sdb) {
        try {
            GetAttributesResult gares1 = sdb.getAttributes(new GetAttributesRequest(domainName, "bala"));
            dumpAttributes(gares1);
            GetAttributesResult gares2 = sdb.getAttributes(new GetAttributesRequest(domainName, "siva"));
            dumpAttributes(gares2);
        } catch (Exception e) {
            System.out.println("put failed, err=" + e.getMessage());
        }
    }

    private static void threadSleep(int sleepInMillis) {
        try {
            Thread.sleep(sleepInMillis);
        } catch (Exception e) {
            System.out.println("thread sleep failed, ignoring...");
        }
    }

    public static void main(String[] args) {
        AmazonSimpleDB sdb = AmazonSimpleDBClientBuilder.defaultClient();
        createDomain(sdb); threadSleep(1000);
        listDomains(sdb); threadSleep(1000);
        put(sdb); threadSleep(1000);
        get(sdb); threadSleep(1000);
        deleteDomain(sdb);
    }
}
