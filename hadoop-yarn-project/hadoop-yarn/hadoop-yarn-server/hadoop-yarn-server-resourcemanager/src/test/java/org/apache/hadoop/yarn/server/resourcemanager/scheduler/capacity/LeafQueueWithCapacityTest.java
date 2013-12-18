/*
 * Copyright 2013 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerAppWithCapacity;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.junit.*;
import static org.junit.Assert.*;

/**
 *
 * @author limin
 */
public class LeafQueueWithCapacityTest {
    
    public LeafQueueWithCapacityTest() {
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void testCanAssign() {
        System.out.println("canAssign");
        FiCaSchedulerAppWithCapacity application = null;
        Priority priority = null;
        FiCaSchedulerNode node = null;
        NodeType type = null;
        RMContainer reservedContainer = null;
        LeafQueueWithCapacity instance = null;
        boolean expResult = false;
        boolean result = instance.canAssign(application, priority, node, type, reservedContainer);
        assertEquals(expResult, result);
        fail("The test case is a prototype.");
    }

    @Test
    public void testAssignContainers() {
        System.out.println("assignContainers");
        Resource clusterResource = null;
        FiCaSchedulerNode node = null;
        LeafQueueWithCapacity instance = null;
        CSAssignment expResult = null;
        CSAssignment result = instance.assignContainers(clusterResource, node);
        assertEquals(expResult, result);
        fail("The test case is a prototype.");
    }
}
