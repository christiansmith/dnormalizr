/**
 * This example uses rare earth elements resource data obtained from usgs.gov.
 */ 


var dzr = require('./lib/index')
  , request = require('request')
  , csv = require('csv');


// Data sources

var opts = { columns: true };

function r1 () { 
  return new csv().fromPath('sample/main.csv', opts).transform(function (data) {
    if (data.longitude !== null && data.latitude !== null) {
      data.location = [parseFloat(data.longitude), parseFloat(data.latitude)];
    }
    return data;
  }); 
}
r1.key = 'rec_id';

function r2 () { return new csv().fromPath('sample/minerals.csv', opts); }
r2.foreignKey = 'rec_id';
r2.property = 'minerals';
r2.key = 'abbr';

function r3 () { return new csv().fromPath('sample/hostrock.csv', opts); }
r3.foreignKey = 'rec_id';
r3.property = 'hostrock';
r3.value = 'value';

function r4 () { return new csv().fromPath('sample/reference.csv', opts); }
r4.foreignKey = 'rec_id';
r4.property = 'references';
r4.value = 'reference';

function r5 () { return new csv().fromPath('sample/resource.csv', opts); }
r5.foreignKey = 'rec_id';
r5.property = 'resource';
r5.value = 'value';

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


dzr.join(r1, r2, r3, r4, r5);
