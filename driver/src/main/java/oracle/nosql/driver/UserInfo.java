/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * On-premises only.
 * <p>
 * A class that encapsulates the information associated with a user including
 * the id and user name in the system.
 */
public class UserInfo {
    final private String id;
    final private String name;

    /**
     * @hidden
     *
     * Constructs an instance of UserInfo as returned by
     * {@link NoSQLHandle#listUsers}.
     * @param id user id
     * @param name user name
     */
    public UserInfo(String id, String name) {
        this.id = id;
        this.name = name;
    }

    /**
     * Returns the id associated with the user.
     *
     * @return the id string
     */
    public String getId() {
        return id;
    }

    /**
     * Returns the name associated with the user.
     *
     * @return the name string
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "id:" + id + ", name:" + name;
    }
}
