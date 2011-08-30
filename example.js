/**
 * This example uses rare earth elements resource data obtained from usgs.gov.
 */ 


var dzr = require('./lib/index')
  , request = require('request')
  , csv = require('csv');


// Data sources

function r1 () { return new csv().fromPath('sample/main.csv', { columns: true}); }
r1.key = 'rec_id';

function r2 () { return new csv().fromPath('sample/minerals.csv', { columns: true}); }
r2.foreignKey = 'rec_id';
r2.property = 'minerals';
r2.key = 'abbr';

function r3 () { return new csv().fromPath('sample/hostrock.csv', { columns: true}); }
r3.foreignKey = 'rec_id';
r3.property = 'hostrock';
r3.value = 'value';


// Dnormalizr Usage

dzr.batchSize = 100;

dzr.on('data', function (data) {
  request({
    uri: 'http://localhost:5984/ree/_bulk_docs',
    method: 'POST',
    json: { docs: data }
  }, function (err, res, body) {
    console.log('Sent ' + data.length + ' docs to CouchDB.');
  });
});

dzr.join(r1, r2, r3);
