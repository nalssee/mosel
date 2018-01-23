var lineByLine = require('n-readlines');
const it = require('iter-tools');
var parse = require('csv-parse/lib/sync');
var _ = require('lodash');
var groupBy = require('./ssql.js').groupBy;

var gBy = function (xs) {
    var result = []
    for (let x of groupBy(xs)){
        result.push(x);
    }
    return result;
};


var filename = process.argv[2];
var columns = process.argv.slice(3);

var liner = new lineByLine(filename);
// throw out the first 8 lines!
for (let i=0; i < 8; i++){
    liner.next();
}

var symbols = gBy(parse(liner.next().toString())[0].slice(1))
if (symbols[0].length !== columns.length){
    throw("Column size mismatch: " + columns.join(", "));
}
symbols = symbols.map((x) => x[0]);

// throw out 5 more!
for (let i=0; i < 5; i++){
    liner.next();
}


// header
console.log(['date', 'id'].concat(columns).join(','));
var line;
while (line = liner.next()){
    line = parse(line.toString())[0];
    date = line[0];
    var a, b;
    for ([a, b] of _.zip(symbols, _.chunk(line.slice(1), columns.length))){
        console.log([date, a].concat(b).join(','));
    }
}

