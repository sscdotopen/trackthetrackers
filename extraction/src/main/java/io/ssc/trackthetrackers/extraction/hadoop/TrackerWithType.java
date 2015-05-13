/**
 * Track the trackers
 * Copyright (C) 2015  Sebastian Schelter, Felix Neutatz, Karim Wadie
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

package io.ssc.trackthetrackers.extraction.hadoop;

public class TrackerWithType {

  private String trackerDomain;
  private int trackerID;
  private TrackingType trackerType;

  public TrackerWithType() {
  }

  public TrackerWithType(String trackerDomain, int trackerID, TrackingType trackerType) {
    super();
    this.trackerDomain = trackerDomain;
    this.trackerID = trackerID;
    this.trackerType = trackerType;
  }

  public String getTrackerDomain() {
    return trackerDomain;
  }

  public void setTrackerDomain(String trackerDomain) {
    this.trackerDomain = trackerDomain;
  }

  public int getTrackerID() {
    return trackerID;
  }

  public void setTrackerID(int trackerID) {
    this.trackerID = trackerID;
  }

  public TrackingType getTrackerType() {
    return trackerType;
  }

  public void setTrackerType(TrackingType trackerType) {
    this.trackerType = trackerType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((trackerDomain == null) ? 0 : trackerDomain.hashCode());
    result = prime * result + trackerID;
    result = prime * result + trackerType.ordinal();
    return result;
  }

  @Override
  public boolean equals(Object obj) {

    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }

    TrackerWithType other = (TrackerWithType) obj;

    if (trackerDomain == null) {
      if (other.trackerDomain != null) {
        return false;
      }
    } else {
      if (!trackerDomain.equals(other.trackerDomain)) {
        return false;
      }
    }
    if (trackerID != other.trackerID) {
      return false;
    }
    if (trackerType != other.trackerType) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "TrackerWithType [trackerDomain=" + trackerDomain + ", trackerID=" + trackerID + ", trackerType="
        + trackerType + "]";
  }
}
