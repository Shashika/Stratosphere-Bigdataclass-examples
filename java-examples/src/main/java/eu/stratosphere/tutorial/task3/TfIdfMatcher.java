/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.tutorial.task3;

import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.tutorial.util.Util;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * This matcher computes the tf-idf weight of every term by combining the results of the previous document and term
 * frequency computation.
 */
public class TfIdfMatcher extends JoinFunction {

    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Computes the Tf-Idf weight of every term by combining the results of the previous document and term
     * frequency computations.
     */
    private final Record result = new Record(3);  //define 3 parameter Record

    @Override
    public void join(Record dfRecord, Record tfRecord, Collector<Record> collector) throws Exception {
        // Implement your solution here
        String wordDf = dfRecord.getField(0, StringValue.class).toString();
        double docFreq = Double.parseDouble(dfRecord.getField(1, IntValue.class).toString());

        IntValue docID = tfRecord.getField(0, IntValue.class);
        String wordTf = tfRecord.getField(1, StringValue.class).toString();
        double termFeq = Double.parseDouble(tfRecord.getField(2, IntValue.class).toString());

        double tf_idf = 0;

        if (wordDf.equals(wordTf)) {
            String word = wordDf;
            tf_idf = termFeq * Math.log(Util.NUM_DOCUMENTS / docFreq);

            result.setField(0, docID);
            result.setField(1, new StringValue(word));
            result.setField(2, new DoubleValue(tf_idf));

            collector.collect(result);
        }

    }
}
