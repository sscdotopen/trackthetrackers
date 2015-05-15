/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz
 * <p/>
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * <p/>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * <p/>
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.extraction.resources;

import org.junit.Test;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;


public class URLHandlerTest {

  @Test
  public void orgUk() {
    try {
      assertTrue(URLHandler.extractHost("lala.co.uk").length() == 10);
    } catch (Exception ex) {
      fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
    }
  }

  @Test
  public void orgUk2() {
    try {
      assertTrue(URLHandler.extractHost("lala.co.uk:8080").length() == 10);
    } catch (Exception ex) {
      fail("Test failed due to a " + ex.getClass().getName() + ": " + ex.getMessage());
    }
  }

  @Test
  public void wrongURL() {
    try {
      URLHandler.extractHost("uk");
      assertTrue(false);
    } catch (Exception ex) {
    }
  }

  @Test
  public void wrongURL2() {
    try {
      URLHandler.extractHost(".uk");
      assertTrue(false);
    } catch (Exception ex) {
    }
  }

  @Test
  public void couldBeURL() {
    assertTrue(!URLHandler.couldBeUrl("user@example.com"));
  }
}
