const m = require('./mosel.js')
var dateFormat = require('dateformat');
const assert = require('assert');
const fs = require('fs');
const _ = require('lodash');

var isNum = m.isNum;
var overlap = m.overlap;
var groupBy = m.groupBy;
var orderBy = m.orderBy;

// ============================================
// helper functions
function addMonth(date, n) {
    var d = new Date(date);
    d.setMonth(d.getMonth() + n);
    return dateFormat(d, "yyyy-mm");
}

function isConsecutive(xs, col) {
    col = col || "Ym";
    for (var i = 0; i < xs.length - 1; i++) {
        if (addMonth(xs[i][col], 1) !== xs[i + 1][col]) {
            return false;
        }
    }
    return true;
}

function arr(x) {
    var result = []
    for (let r of x) {
        result.push(r);
    }
    return result;
}


function sum(xs) {
    return xs.reduce((a, x) => a + x);
}


m.setdir('data');

m.connect('test.db', (c) => {
    c.load('orders.csv');
    c.load('products.csv');
});


m.connect('test.db', (c) => {
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
    }(), 'CategoryID');

    assert.deepEqual(c._getPrimaryKeys('productsAvg'), ['CategoryID']);

    var rs = arr(c.fetch('productsAvg'));
    assert.deepEqual(
        rs.map((r) => r.aggPrice), [455.75, 276.75, 327.08, 287.3, 141.75, 324.04, 161.85, 248.19]
    );
    assert.deepEqual(rs.map((x) => x.n), [12, 12, 13, 10, 7, 6, 5, 12])
});


m.connect('test.db', (c) => {
    c.register(addMonth);
    c.createAs('orders1', "select *, addMonth(OrderDate, 0) as Ym from orders");
    var ls = [];
    for (let rs of c.fetch('orders1', {
            overlap: [5, 2],
            groupBy: "Ym"
        })) {
        ls.push(groupBy(rs, 'Ym').map((x) => x.length));
    }
    var ls2 = [
        [22, 25, 23, 26, 25],
        [23, 26, 25, 31, 33],
        [25, 31, 33, 11],
        [33, 11]
    ];
    assert.deepEqual(ls, ls2);

    var xs = [];
    for (let rs of c.fetch('orders', {overlap: [10, 3]})){
        if (rs.length !== 10){
            xs.push(rs.length);
        }
    }
    assert.deepEqual(xs, [7, 4, 1]);
});


m.connect('test.db', (c) => {
    for (let rs of c.fetch('products', {
            groupBy: 'CategoryID',
            overlap: 3
        })) {
        assert.equal(typeof rs[0].ProductName, 'string');
        // must not affect the following to the next element
        for (let r of rs) {
            r.ProductName = 0;
        }
    }
});


m.connect('test.db', (c) => {
    var n1 = arr(c.fetch('orders1')).length;
    c.toCsv('orders1');
    c.drop('orders1');
    c.load('orders1.csv');
    var n2 = arr(c.fetch('orders1')).length;
    assert.equal(n1, n2);
    fs.unlinkSync('data/orders1.csv');
});


m.connect('test.db', (c) => {
    c.split('products', {
        'test1.db': 'CategoryID < 5',
        'test2.db': 'CategoryID >= 5'
    });

    m.connect('test1.db', (c1) => {
        assert.equal(arr(c1.fetch('products')).length, 47);
    });

    m.connect('test2.db', (c2) => {
        assert.equal(arr(c2.fetch('products')).length, 30);
    });

    c.drop('products');
    c.collect('products', ['test1.db', 'test2.db']);
    assert.equal(arr(c.fetch('products')).length, 77);

});

assert.equal(isNum(3), 1);
assert.equal(isNum("abc"), 0);
m.connect('test.db', (c) => {
    var rs = orderBy(arr(c.fetch('products')), 'CategoryID');
    assert.deepEqual(groupBy(rs, 'CategoryID').map((x) => x.length), [12, 12, 13, 10, 7, 6, 5, 12]);

    var result = []
    for (let x of overlap(groupBy(rs, 'CategoryID'), 5, 2)){
        result.push(x.length);
    }
    assert.deepEqual(result, [ 54, 41, 30, 17 ]);
});


m.connect('test.db', (c) => {
    c.load('customers.csv');
    c.join('foo', [
        ['customers','customerName', 'customerID'],
        ['orders', 'orderID' , 'customerid']
    ]);
    assert.equal(arr(c.fetch('foo')).length, 213);
});


fs.unlinkSync('data/test.db');
fs.unlinkSync('data/test1.db');
fs.unlinkSync('data/test2.db');