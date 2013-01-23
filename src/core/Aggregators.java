// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Utility class that provides common, generally useful aggregators.
 */
public final class Aggregators {

  /** Aggregator that sums up all the data points. */
  public static final Aggregator SUM = new Sum();

  /** Aggregator that returns the minimum data point. */
  public static final Aggregator MIN = new Min();

  /** Aggregator that returns the maximum data point. */
  public static final Aggregator MAX = new Max();

  /** Aggregator that returns the average value of the data point. */
  public static final Aggregator AVG = new Avg();

  /** Aggregator that returns the Standard Deviation of the data points. */
  public static final Aggregator DEV = new StdDev();
  
  /** Aggregator that returns the 80th percentile of the data points. */
  public static final Aggregator PCT80 = new Percentile(80);
  
  /** Aggregator that returns the 85th percentile of the data points. */
  public static final Aggregator PCT85 = new Percentile(85);
  
  /** Aggregator that returns the 90th percentile of the data points. */
  public static final Aggregator PCT90 = new Percentile(90);
  
  /** Aggregator that returns the 95th percentile of the data points. */
  public static final Aggregator PCT95 = new Percentile(95);
  
  public static final Aggregator PCT50 = new Percentile(50);

  /** Maps an aggregator name to its instance. */
  private static final HashMap<String, Aggregator> aggregators;

  static {
    aggregators = new HashMap<String, Aggregator>(5);
    aggregators.put("sum", SUM);
    aggregators.put("min", MIN);
    aggregators.put("max", MAX);
    aggregators.put("avg", AVG);
    aggregators.put("dev", DEV);
    aggregators.put("pct80", PCT80);
    aggregators.put("pct85", PCT85);
    aggregators.put("pct90", PCT90);
    aggregators.put("pct95", PCT95);
    aggregators.put("pct50", PCT50);
  }

  private Aggregators() {
    // Can't create instances of this utility class.
  }

  /**
   * Returns the set of the names that can be used with {@link #get get}.
   */
  public static Set<String> set() {
    return aggregators.keySet();
  }

  /**
   * Returns the aggregator corresponding to the given name.
   * @param name The name of the aggregator to get.
   * @throws NoSuchElementException if the given name doesn't exist.
   * @see #set
   */
  public static Aggregator get(final String name) {
    final Aggregator agg = aggregators.get(name);
    if (agg != null) {
      return agg;
    }
    throw new NoSuchElementException("No such aggregator: " + name);
  }
  
  private static final class Percentile implements Aggregator {
    
    private final int p;
    private final double pp;
    
    public Percentile(int p) {
      this.p = p;
      pp = p / 100d;
    }
      
    public long runLong(final Longs values) {
      // the naive approach to compute percentile
      List<Long> valuesList = new ArrayList<Long>();
      valuesList.add(new Long(values.nextLongValue()));
      while (values.hasNextValue()) {
        valuesList.add(new Long(values.nextLongValue()));
      }
      Collections.sort(valuesList);
      int n = (int) Math.round(pp * valuesList.size() + 0.5d);
      
      return valuesList.get(n - 1);
    }

    public double runDouble(final Doubles values) {
      // the naive approach to compute percentile
      List<Double> valuesList = new ArrayList<Double>();
      valuesList.add(new Double(values.nextDoubleValue()));
      while (values.hasNextValue()) {
        valuesList.add(new Double(values.nextDoubleValue()));
      }
      Collections.sort(valuesList);
      int n = (int) Math.round(pp * valuesList.size() + 0.5d);
      
      return valuesList.get(n - 1);
    }
      
    public String toString() {
      return "pct" + p;
    }
      
  }

  private static final class Sum implements Aggregator {

    public long runLong(final Longs values) {
      long result = values.nextLongValue();
      while (values.hasNextValue()) {
        result += values.nextLongValue();
      }
      return result;
    }

    public double runDouble(final Doubles values) {
      double result = values.nextDoubleValue();
      while (values.hasNextValue()) {
        result += values.nextDoubleValue();
      }
      return result;
    }

    public String toString() {
      return "sum";
    }

  }

  private static final class Min implements Aggregator {

    public long runLong(final Longs values) {
      long min = values.nextLongValue();
      while (values.hasNextValue()) {
        final long val = values.nextLongValue();
        if (val < min) {
          min = val;
        }
      }
      return min;
    }

    public double runDouble(final Doubles values) {
      double min = values.nextDoubleValue();
      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (val < min) {
          min = val;
        }
      }
      return min;
    }

    public String toString() {
      return "min";
    }

  }

  private static final class Max implements Aggregator {

    public long runLong(final Longs values) {
      long max = values.nextLongValue();
      while (values.hasNextValue()) {
        final long val = values.nextLongValue();
        if (val > max) {
          max = val;
        }
      }
      return max;
    }

    public double runDouble(final Doubles values) {
      double max = values.nextDoubleValue();
      while (values.hasNextValue()) {
        final double val = values.nextDoubleValue();
        if (val > max) {
          max = val;
        }
      }
      return max;
    }

    public String toString() {
      return "max";
    }

  }

  private static final class Avg implements Aggregator {

    public long runLong(final Longs values) {
      long result = values.nextLongValue();
      int n = 1;
      while (values.hasNextValue()) {
        result += values.nextLongValue();
        n++;
      }
      return result / n;
    }

    public double runDouble(final Doubles values) {
      double result = values.nextDoubleValue();
      int n = 1;
      while (values.hasNextValue()) {
        result += values.nextDoubleValue();
        n++;
      }
      return result / n;
    }

    public String toString() {
      return "avg";
    }
  }

  /**
   * Standard Deviation aggregator.
   * Can compute without storing all of the data points in memory at the same
   * time.  This implementation is based upon a
   * <a href="http://www.johndcook.com/standard_deviation.html">paper by John
   * D. Cook</a>, which itself is based upon a method that goes back to a 1962
   * paper by B.  P. Welford and is presented in Donald Knuth's Art of
   * Computer Programming, Vol 2, page 232, 3rd edition
   */
  private static final class StdDev implements Aggregator {

    public long runLong(final Longs values) {
      double old_mean = values.nextLongValue();

      if (!values.hasNextValue()) {
        return 0;
      }

      long n = 2;
      double new_mean = 0;
      double variance = 0;
      do {
        final double x = values.nextLongValue();
        new_mean = old_mean + (x - old_mean) / n;
        variance += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      } while (values.hasNextValue());

      return (long) Math.sqrt(variance / (n - 1));
    }

    public double runDouble(final Doubles values) {
      double old_mean = values.nextDoubleValue();

      if (!values.hasNextValue()) {
        return 0;
      }

      long n = 2;
      double new_mean = 0;
      double variance = 0;
      do {
        final double x = values.nextDoubleValue();
        new_mean = old_mean + (x - old_mean) / n;
        variance += (x - old_mean) * (x - new_mean);
        old_mean = new_mean;
        n++;
      } while (values.hasNextValue());

      return Math.sqrt(variance / (n - 1));
    }

    public String toString() {
      return "dev";
    }
  }

}
