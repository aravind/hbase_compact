/**
 * This file is part of hbaseadmin.
 * Copyright (C) 2011 StumbleUpon, Inc.
 *
 * This program is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version. This program is distributed in the hope that it
 * will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details. You should have received a copy
 * of the GNU Lesser General Public License along with this program. If not,
 * see <http: *www.gnu.org/licenses/>.
 */

package com.stumbleupon.hbaseadmin;

import java.util.Date;
import java.text.SimpleDateFormat;

class Utils {

  public static String dateString(final Date d, final String format) {
    final SimpleDateFormat df = new SimpleDateFormat(format);
    return df.format(d);
  }

  /**
   * sleeps for sleep_between_checks milliseconds untill the current
   * time-stamp is between start and stop.
   */
  public static void waitTillTime(final String start,
                                  final String stop,
                                  final int sleep_between_checks)
    throws InterruptedException {

    String now = dateString(new Date(), "HHmm");

    while ((now.compareTo(start) < 0) ||
           (now.compareTo(stop) > 0)) {
      Thread.sleep(sleep_between_checks);
      now = dateString(new Date(), "HHmm");
    }
  }
}
