/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * A simple main for the driver jar file. At this time it only handles a
 * "version" command. If it ever is extended for other arguments it should
 * be better abstracted for multiple commands
 *
 * @hidden
 */
public class DriverMain {

    private static String libraryVersion = findVersion();
    private static String buildId = findBuildId();

    /**
     * Delegates to Command object named by first arg.  If no args, delegates
     * to 'help' Command.
     * @param args the command line arguments
     * @throws Exception on failure
     */
    public static void main(String args[])
        throws Exception {

        if (args.length == 0 || args[0].equalsIgnoreCase("-version")) {
            System.out.println(libraryVersion);
            return;
        }
        if (args[0].equalsIgnoreCase("-build")) {
            System.out.println(buildId);
            return;
        }

        usage();
    }

    public static String getLibraryVersion() {
        return libraryVersion;
    }

    /**
     * Top-level usage command.
     */
    private static void usage() {
        final StringBuilder builder = new StringBuilder();
        builder.append("java -jar <path-to-driver-jar> [-version|-build]");
        System.err.println(builder);
        usageExit();
    }

    /**
     * Does System.exit on behalf of all usage commands.
     */
    private static void usageExit() {
        System.exit(2);
    }

    /**
     * Pulls the version string from the manifest. The version is added
     * by maven.
     */
    private static String findVersion() {
        return SDKVersion.VERSION;
    }

    /**
     * Pulls the version string from the manifest. See build.xml for
     * how this is constructed.
     */
    private static String findBuildId() {
        Attributes attrs = findAttributes();
        if (attrs != null) {
            return attrs.getValue("Build-Id");
        }
        return null;
    }

    /**
     * Pulls the version string from the manifest. See build.xml for
     * how this is constructed.
     */
    private static Attributes findAttributes() {
        try {
            Enumeration<URL> resources = DriverMain.class.getClassLoader()
                .getResources("META-INF/MANIFEST.MF");
            while (resources.hasMoreElements()) {
                Manifest manifest =
                    new Manifest(resources.nextElement().openStream());
                Attributes attrs = manifest.getMainAttributes();
                String title =
                    attrs.getValue(Attributes.Name.IMPLEMENTATION_TITLE);
                if (title != null &&
                    title.toLowerCase().contains("sdk for java")) {
                    return attrs;
                }
            }
        } catch (IOException e) {
        }
        return null;
    }
}
