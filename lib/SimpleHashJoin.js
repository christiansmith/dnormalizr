/*
 * Copyright(c) 2011 Christian Smith <smith@anvil.io>
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter
  , util = require('util');

/**
 * SimpleHashJoin inherits from EventEmitter. It emits results 
 * of the join, as they are computed.
 */

function SimpleHashJoin () {
  this.hash = {};
  EventEmitter.call(this);
}

util.inherits(SimpleHashJoin, EventEmitter);

module.exports = SimpleHashJoin;

/**
 * The join method.
 */

SimpleHashJoin.prototype.join = function() {
  // Create an array of relations from the arguments,
  // take the first argument as the basis for our documents,
  // initialize a counter to track the number of completed
  // joins, initialize "hash" to an empty object to hold
  // intermediate results, and last but not least create 
  // a reference to "this" that will be accessible to functions
  // defined within shj.join.
  var relations = Array.prototype.slice.call(arguments)
    , primary = relations.shift()
    , completed = 0
    , hash = this.hash
    , hashKeys = []
    , batch = []
    , self = this;

  // Create a "stream" of documents from the primary relation.
  documents = primary();

  // Read the stream into hash.
  documents.on('data', function (data) {
    var key = data[primary.key];
    hash[key] = data;
    hashKeys.push(key);
  });

  // Perform the joins
  documents.on('end', function () {

    // Start processing the remaining relations in parallel
    relations.forEach(function (relation) {

      // Create a stream from the relation function
      var tuples = relation()
        , property = relation.property;

      // Join the stream contents into the documents in hash
      tuples.on('data', function (data) {

        // Lookup the document in hash.
        var key = data[relation.foreignKey]
          , document = hash[key];

        if (document) {

          // Initialize the container for joined data. If a
          // key is defined on the relation, set an empty 
          // object, otherwise set an empty array.
          if (!document[property]) {
            document[property] = (relation.key) ? {} : [];
          }

          // Remove the foreign key from data.
          delete data[relation.foreignKey];

          // Add the tuple-object (or a value from it) into 
          // the container we just created.
          (relation.key)
            ? document[property][data[relation.key]] = data
            : (relation.value)
                ? document[property].push(data[relation.value])
                : document[property].push(data);

          // Remove the property key from data.
          delete data[relation.key];

          // Initialize the value of relation.previousKey to the
          // foreignKey of the first tuple.
          if (!relation.previousKey) {
            relation.previousKey = key;
          }

          // Check for a change in the foreign key compared to 
          // the previous key.
          if (key !== relation.previousKey) {
            
            // Get the indices of document keys in hashKeys
            var keyIndex = hashKeys.indexOf(key)
              , prevKeyIndex = hashKeys.indexOf(relation.previousKey);
            
            // Use the indices to get a range of keys (because the inputs
            // are sorted) that includes any keys that do not match any tuples.
            // We need to increment join counters for the "skipped" document as
            // well as the previous document.
            hashKeys.slice(prevKeyIndex - 1, keyIndex - 1).forEach(function (k) {

              // Reference the document
              var previous = hash[k];
              
              // Initialize a counter to track the number of joins
              // completed for the document.
              if (!previous._completed) previous._completed = 0;

              // Increment the join counter.
              previous._completed += 1;

              // If all the joins have been performed for this 
              // document, delete the counter and emit the document.
              if (previous._completed === relations.length) {
                delete previous._completed;

                // Batch or emit the completed document.
                if (self.batchSize) {
                  batch.push(previous);

                  // Emit the entire batch once it is full, 
                  // then reset batch.
                  if (batch.length >= self.batchSize) {
                    self.emit('data', batch);
                    batch = [];
                  }
                } else {
                  self.emit('data', previous);
                }
              }
            });

            // Set the previous key to the current key.
            relation.previousKey = key;
          }
        }
      });

      tuples.on('end', function () {
        // Track the number of input streams that have ended
        // and emit "end" when all joins are complete.
        completed += 1;
        if (completed === relations.length) {
  
          var prevKeyIndex = hashKeys.indexOf(relation.previousKey)
            , lastKeyIndex = hashKeys.length - 1
            , lastKey = hashKeys[lastKeyIndex];

          hashKeys.slice(prevKeyIndex - 1, lastKeyIndex + 1).forEach(function (key) {
            var document = hash[key];
            delete document._completed;

            if (self.batchSize) {
              batch.push(document);

              if (batch.length >= self.batchSize || key === lastKey) {
                self.emit('data', batch);
                batch = [];
              }
            } else {
              self.emit('data', document);
            }
          });

          self.emit('end');
        }
      });
    });
  });
};
