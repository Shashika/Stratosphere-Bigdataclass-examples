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
package eu.stratosphere.tutorial.task4;

import java.util.Iterator;
import java.util.List;

import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

/**
 * This reducer groups by document ID and creates a {@link WeightVector} for each document.
 */
public class WeightVectorReducer extends ReduceFunction {

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Creates a Tf-Idf {@link WeightVector} for each document.
	 */
	@Override
	public void reduce(Iterator<Record> records, Collector<Record> collector) throws Exception {
		// Implement your solution here
        Record record = null;
        int docID = 0;
        List<Double> weights;
        Double sum = 0.0;

        while(records.hasNext()){

            sum = 0.0;
            record = records.next();
            WeightVector weightVector = record.getField(0,WeightVector.class);
            docID = weightVector.getDocID();
            weights = weightVector.getWeightList();

            for(int co = 0; co < weights.size(); co++){
                 sum += weights.get(co);

            }
        }
        record.setField(docID,new DoubleValue(sum));
        collector.collect(record);

	}
}
