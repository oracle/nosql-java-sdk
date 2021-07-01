/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Durability;

/**
 * @hidden
 *
 * Represents a base class for operations that support a
 * {@link Durability} setting.
 */
public abstract class DurableRequest extends Request {

    private Durability durability;

    protected DurableRequest() {}

    protected void setDurabilityInternal(Durability durability) {
        this.durability = durability;
    }

    /**
     * Returns the durability setting for this operation.
     * On-prem only.
     *
     * @return durability, if set. Otherwise null.
     */
    public Durability getDurability() {
        return durability;
    }

    /**
     * @hidden
     */
    @Override
    public boolean doesWrites() {
        return true;
    }
}
