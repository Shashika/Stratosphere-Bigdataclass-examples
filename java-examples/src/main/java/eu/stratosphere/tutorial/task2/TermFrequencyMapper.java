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
package eu.stratosphere.tutorial.task2;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.tutorial.util.Util;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

import java.util.*;


/**
 * This mapper computes the term frequency for each term in a document.
 * <p>
 * The term frequency of a term in a document is the number of times the term occurs in the respective document. If a
 * document contains a term three times, the term has a term frequency of 3 (for this document).
 * <p>
 * Example:
 * 
 * <pre>
 * Document 1: "Big Big Big Data"
 * Document 2: "Hello Big Data"
 * </pre>
 * 
 * The term frequency of "Big" in document 1 is 3 and 1 in document 2.
 * <p>
 * The map method will be called independently for each document.
 */
public class TermFrequencyMapper extends MapFunction {

	// ----------------------------------------------------------------------------------------------------------------

	/**
	 * Splits the document into terms and emits a PactRecord (docId, term, tf) for each term of the document.
	 * <p>
	 * Each input document has the format "docId, document contents".
	 */
    private final Record result = new Record(3);  //define 3 parameter Record

	@Override
	public void map(Record record, Collector<Record> collector) {
        // Document with format "docId, document contents"
        String document = record.getField(0, StringValue.class).toString();

        // need to avoid the docId
        String data[] = document.split(",");
        int docID = Integer.parseInt(data[0]);

        //String docID = data[0];
        document = data[1];

        //1,Big Hello to Stratosphere! :-)   ->    1 big hello to stratosphere
        document = document.replaceAll("\\W", " ").toLowerCase();

        StringTokenizer tokenizer = new StringTokenizer(document);
        HashSet<String> stopWords = Util.STOP_WORDS;
        Map<String,Integer> map = new HashMap<String,Integer>();   //to identify the frequency of each word in the document

        int co = 1;
        while (tokenizer.hasMoreElements()) {

            String word = tokenizer.nextToken();
            if (stopWords.contains(word.toString())) {
                continue;
            }

            if(map.containsKey(word)){     //if the word added previously increment the count by one
                co++;
                map.put(word,co);
            }
            else {                  //add a new word to the map
                co = 1;
                map.put(word,co);
            }
        }

        Iterator iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry pairs = (Map.Entry)iterator.next();
            String word = pairs.getKey().toString();
            int occur = Integer.parseInt(pairs.getValue().toString());

            result.setField(0,new IntValue(docID));
            result.setField(1, new StringValue(word));
            result.setField(2, new IntValue(occur));
            collector.collect(result);
        }
    }
}