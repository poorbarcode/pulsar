package test.org.apache.pulsar;

import org.apache.pulsar.admin.cli.PulsarAdminTool;

public class PulsarAdminToolInvoker {

    public static void main(String[] ignore) throws Throwable{
        String commandLime = "persistent list public/default";

        String pulsarHome = System.getProperty("user.dir");
        String clientConfigFile = pulsarHome + "/pulsar-standalone-local/src/test/resources/client.conf";
        String[] args = (clientConfigFile + " " + commandLime).split(" ");
        PulsarAdminTool.main(args);
    }
}
