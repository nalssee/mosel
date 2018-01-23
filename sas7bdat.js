// sas7bdat to csv
var path = require('path');
var fs = require('fs');
const SAS7BDAT = require('sas7bdat');

var filename = process.argv[2];
var [fname, ext] = filename.split('.');

var fin = SAS7BDAT.createReadStream(filename);
var fout = fs.createWriteStream(fname + '.csv');

fin.on('data', row => fout.write(row.join(',') + '\n'));


fin.on('error', err => console.log(err));
fin.on('end', () => {
    console.log("Written: ", fname + '.csv');
    fout.close();
});
