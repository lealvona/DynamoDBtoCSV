var program = require('commander');
var AWS = require('aws-sdk');
AWS.config.loadFromPath('./config.json');
var dynamoDB = new AWS.DynamoDB();
var headers = [];
var final_table = [];
const fs = require('fs');


program.version('0.0.1')
    .option('-t, --table [tablename]', 'Add the table you want to output to csv')
    .option("-d, --describe")
    .parse(process.argv);

if (!program.table) {
    console.log("You must specify a table");
    program.outputHelp();
    process.exit(1);
}

var query = {
    "TableName": program.table,
    "Limit": 1000,
};


function describeTable(query) {

    dynamoDB.describeTable({
        "TableName": program.table
    }, function(err, data) {

        if (!err) {

            console.dir(data.Table);

        } else console.dir(err);
    });
}

function scanDynamoDB(query) {

    dynamoDB.scan(query, function(err, data) {

        if (!err) {
            //printout(data.Items) // Print out the subset of results.
            isrc_list = pivot_array_on_isrc(data.Items); // Print out the subset of results.
            final_table.push(create_table(isrc_list));
            if (data.LastEvaluatedKey) { // Result is incomplete; there is more to come.
                query.ExclusiveStartKey = data.LastEvaluatedKey;
                scanDynamoDB(query);
            };
        } else console.dir(err);
    });

    // return final_table;
}

function arrayToCSV(array_input) {
    var string_output = "";
    for (var i = 0; i < array_input.length; i++) {
        array_input[i] = array_input[i].replace(/\r?\n/g, ""); // strip newlines
        string_output += ('"' + array_input[i].replace(/\"/g, '\\"') + '"'); //add to string and remove quotes
        if (i != array_input.length - 1) string_output += ","
    };
    return string_output;
}

function printout(items) {
    var headersMap = {};
    var values;
    var header;
    var value;

    if (headers.length == 0) {
        if (items.length > 0) {
            for (var i = 0; i < items.length; i++) {
                for (var key in items[i]) {
                    headersMap[key] = true;
                }
            }
        }
        for (var key in headersMap) {
            headers.push(key);
        }
        console.log(arrayToCSV(headers))
    }

    for (index in items) {
        values = [];
        for (i = 0; i < headers.length; i++) {
            value = "";
            header = headers[i];
            // Loop through the header rows, adding values if they exist
            if (items[index].hasOwnProperty(header)) {
                if (items[index][header].N) {
                    value = items[index][header].N;
                } else if (items[index][header].S) {
                    value = items[index][header].S;
                } else if (items[index][header].SS) {
                    value = items[index][header].SS.toString();
                } else if (items[index][header].NS) {
                    value = items[index][header].NS.toString();
                } else if (items[index][header].B) {
                    value = items[index][header].B.toString('base64');
                } else if (items[index][header].M) {
                    value = JSON.stringify(items[index][header].M);
                } else if (items[index][header].L) {
                    value = JSON.stringify(items[index][header].L);
                } else if (items[index][header].BOOL !== undefined) {
                    value = items[index][header].BOOL.toString();
                }
            }
            values.push(value)
        }
        console.log(arrayToCSV(values))
    }
}

function pivot_array_on_isrc(items) {
    var values;
    var value;
    var isrc_obj = {};
    var isrc;

    for (index in items) {
        values = [];
        isrc = items[index].isrc.S;
        isrc_obj[isrc] = {};
        value = "";
        //header = headers[i];

        terr_list = items[index].territories.M

        for (terr in terr_list) {
            if (terr_list.hasOwnProperty(terr)) {
                tuid_obj = items[index].territories.M[terr].M;

                if (typeof(isrc_obj[isrc][tuid_obj.tuid.N]) == 'undefined') {
                    isrc_obj[isrc][tuid_obj.tuid.N] = [];
                }

                isrc_obj[isrc][tuid_obj.tuid.N].push(terr);
                // value = JSON.stringify(items[index][header].M)
            }
        }
    }

    return isrc_obj;
}

function create_table(isrc_list_obj) {
    var output_table = [];
    var output_row = {};

    for (isrc in isrc_list_obj) {
        if (!isrc_list_obj.hasOwnProperty(isrc)) continue;

        // Do something with `child`
        // console.log(isrc)
        for (tuid in isrc_list_obj[isrc]) {
            if (!isrc_list_obj.hasOwnProperty(isrc)) continue;

            output_row['isrc'] = isrc;
            output_row['tuid'] = tuid;
            output_row['territories'] = isrc_list_obj[isrc][tuid].join();

            output_table.push(output_row);
        }
    }

    return output_table;
}

if (program.describe) describeTable(query);
else scanDynamoDB(query);

for (key in final_table) {
    if (!final_table.hasOwnProperty(key)) continue;
    for (k in final_table[key]) {
        if (!final_table[key].hasOwnProperty(k)) continue;
        console.log(final_table[key][k]);
    }
}
