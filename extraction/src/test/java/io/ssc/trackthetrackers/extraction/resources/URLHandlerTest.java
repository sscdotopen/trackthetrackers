/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.ssc.trackthetrackers.extraction.resources;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class URLHandlerTest {

  @Test
  public void orgUk() {
    assertTrue(URLHandler.isValidDomain("lala.co.uk"));
  }

  @Test
  public void wrongURL() {
    assertTrue(!URLHandler.isValidDomain("uk"));
  }

  @Test
  public void wrongURL2() {
    assertTrue(!URLHandler.isValidDomain(".uk"));
  }

  @Test
  public void couldBeURL() {
    assertTrue(!URLHandler.couldBeUrl("user@example.com"));
  }
}
