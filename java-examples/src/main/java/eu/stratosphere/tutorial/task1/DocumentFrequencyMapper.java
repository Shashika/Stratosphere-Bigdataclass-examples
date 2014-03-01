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
package eu.stratosphere.tutorial.task1;

import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.tutorial.util.Util;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

import java.util.HashSet;
import java.util.StringTokenizer;

/**
 * This mapper is part of the document frequency computation.
 * <p/>
 * The document frequency of a term is the number of documents it occurs in. If a document contains a term three times,
 * it is counted only once for this document. But if another document contains the word as well, the overall frequency
 * is two.
 * <p/>
 * Example:
 * <p/>
 * <pre>
 * Document 1: "Big Big Big Data"
 * Document 2: "Hello Big Data"
 * </pre>
 * <p/>
 * The document frequency of "Big" is 2, because it appears in two documents (even though it appears four times in
 * total). "Hello" has a document frequency of 1, because it only appears in document 2.
 * <p/>
 * The map method will be called independently for each document.
 */
public class DocumentFrequencyMapper extends MapFunction {

    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Splits the document into terms and emits a PactRecord (term, 1) for each term of the document.
     * <p/>
     * Each input document has the format "docId, document contents".
     * <p/>
     * Example:
     * <p/>
     * <pre>
     * 1,Gartner's definition (the 3Vs) is still widely used
     * </pre>
     * <p/>
     * The document ID of the document is 1 (start of line before the comma). The terms to be extracted are:
     * <ul>
     * <li>gartner</li>
     * <li>s</li>
     * <li>definition</li>
     * <li>3vs</li>
     * <li>still</li>
     * <li>widely</li>
     * </ul>
     * Note that the stop words "the" and "is" have been removed and everything has been lower cased.
     */
    @Override
    public void map(Record record, Collector<Record> collector) {
        // Document with format "docId, document contents"
        String document = record.getField(0, StringValue.class).toString();

        // need to avoid the docId
        String data[] = document.split(",");
        document = data[1];

        //1,Big Hello to Stratosphere! :-)   ->    1 big hello to stratosphere
        document = document.replaceAll("\\W", " ").toLowerCase();

        StringTokenizer tokenizer = new StringTokenizer(document);

        HashSet<String> stopWords = Util.STOP_WORDS;
        HashSet<String> repeatWords = new HashSet<String>();

        //not to occur same word more than one time in a document
        while (tokenizer.hasMoreElements()) {
            String word = tokenizer.nextToken();
            repeatWords.add(word);
        }

        for(String word : repeatWords){
            //to check whether avoided words are there
            if (stopWords.contains(word.toString())) {
                continue;
            }
            collector.collect(new Record(new StringValue(word), new IntValue(1)));
        }
    }
}