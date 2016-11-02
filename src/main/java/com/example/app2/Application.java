/**
 * Put your copyright and license info here.
 */
package com.example.app2;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Creating an example application with three operators
    // The last operator is slowing down the DAG
    // With the use of the stats listener the input operator is slowed when the window difference crosses a threshold

    RandomNumberGenerator randomGenerator = dag.addOperator("RandomGenerator", RandomNumberGenerator.class);
    PassThroughOperator<Double> passThrough = dag.addOperator("PassThrough", PassThroughOperator.class);
    SlowConsoleOperator<Double> console = dag.addOperator("SlowConsole", SlowConsoleOperator.class);

    // Important use the same stats listener object for all operators so that we can centrally collect stats and make
    // the decision
    StatsListener statsListener = new ThrottlingStatsListener();
    Collection<StatsListener> statsListeners = Lists.newArrayList(statsListener);
    dag.setAttribute(randomGenerator, Context.OperatorContext.STATS_LISTENERS, statsListeners);
    dag.setAttribute(passThrough, Context.OperatorContext.STATS_LISTENERS, statsListeners);
    dag.setAttribute(console, Context.OperatorContext.STATS_LISTENERS, statsListeners);

    // Increase timeout for the slow operator, this specifies the maximum timeout for an operator to process a window
    // It is specified in number of windows, since 1 window is 500ms, 30 windows is 30 * 60 * 2 = 3600 windows
    dag.setAttribute(console, Context.OperatorContext.TIMEOUT_WINDOW_COUNT, 3600);

    // If there are unifiers that are slow then set timeout for them
    // dag.setUnifierAttribute(passThrough.output, Context.OperatorContext.TIMEOUT_WINDOW_COUNT, 3600);

    dag.addStream("randomData", randomGenerator.out, passThrough.input);
    dag.addStream("passData", passThrough.output, console.input);
  }
}
