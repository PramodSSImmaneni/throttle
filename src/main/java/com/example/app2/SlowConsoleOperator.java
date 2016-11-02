package com.example.app2;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.base.Throwables;

/**
 * Created by pramod on 9/27/16.
 */
public class SlowConsoleOperator<T> extends BaseOperator {

    // Modify sleep time dynamically while app is running to increase and decrease sleep time
    long sleepTime = 5;

    public transient final DefaultInputPort<T> input = new DefaultInputPort<T>() {
        @Override
        public void process(T t) {
            // Introduce an aritificial delay for every tuple
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                throw Throwables.propagate(e);
            }
            System.out.println(t);
        }
    };

    public long getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(long sleepTime) {
        this.sleepTime = sleepTime;
    }
}
