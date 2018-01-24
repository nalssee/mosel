var m = require('./mosel.js');
const assert = require('assert');
const fs = require('fs');
const cluster = require('cluster');


function sum(xs) {
    return xs.reduce((a, x) => a + x);
}

function arr(x) {
    var result = []
    for (let r of x) {
        result.push(r);
    }
    return result;
}


m.setdir('data');


if (cluster.isMaster) {
    m.connect('test.db', (c) => {
        c.load('products.csv');
        c.split('products', {
            'test1.db': 'CategoryID <= 4',
            'test2.db': 'CategoryID > 4'
        });
    });
}


m.pconnect({
    'test1.db': null,
    'test2.db': null
}, (c, arg) => {
    c.new('productsAvg', function* () {
        for (let rs of c.fetch("products", {
                groupBy: "CategoryID"
            })) {
            var r = {};
            r.CategoryID = rs[0].CategoryID;
            r.aggPrice = sum(rs.map((x) => x.Price));
            r.n = rs.length
            yield r;
        }
    }());

}, () => {
    m.connect('test.db', (c) => {
        c.collect('productsAvg', ['test1.db', 'test2.db']);
        assert.equal(arr(c.fetch('productsAvg')).length, 8);
    });
    fs.unlinkSync('data/test.db');
    fs.unlinkSync('data/test1.db');
    fs.unlinkSync('data/test2.db');
});

