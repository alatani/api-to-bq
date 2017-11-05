// Overwrite here with your settings
var projectId = 'XXXXXXXX'
var dataset_name = 'dataset_name'
var table_name = 'table_name'


var gcloud = require('gcloud')({
  projectId: projectId
});


exports.get_http_to_bq = function(context, data){

  var register_bq = function (data){
    var bigquery = gcloud.bigquery();
    var dataset = bigquery.dataset(dataset_name);
    var table = dataset.table(table_name);
     
    table.insert(data, function(err, insertErrors, apiResponse){
      // insert callback
    });

  }


  var fetch_data = function(callback){
  }

     
   context.success();
}

