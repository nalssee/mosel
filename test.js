const m = require('../mosel.js')
var dateFormat = require('dateformat');

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
    col = col || "ym";
    for (var i = 0; i < xs.length - 1; i++) {
        if (addMonth(xs[i][col], 1) !== xs[i + 1][col]) {
            return false;
        }
    }
    return true;
}


// ==========================================================


m.connect('test.db', (c) => {
    c.load('orders.csv');
    c.load('products.csv');
});





