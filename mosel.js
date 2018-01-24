var fs = require('fs');
const it = require('iter-tools');
var path = require('path');
var parse = require('csv-parse/lib/sync');
var Database = require('better-sqlite3');
var lineByLine = require('n-readlines');
var cluster = require('cluster');
var _ = require('lodash');


// current working directory
var CWD = '';


function Dbase(dbfile) {
    this.db = new Database(dbfile);
    // another connection, for fetching
    this.db2 = new Database(dbfile, {
        readonly: true
    });
}


Dbase.prototype.load = function (fname, tname, pkeys) {
    var self = this;
    _withTrans(self.db, () => {
        tname = tname || fname.split('.')[0];
        if (self.getTables().indexOf(tname) === -1) {
            var liner = new lineByLine(path.join(CWD, fname));
            var firstLine = parse(liner.next().toString())[0];
            self.db.prepare(_cstmt(tname, firstLine, pkeys)).run();
            var istmt = self.db.prepare(_istmt(tname, firstLine.length));
            var line;
            while (line = liner.next()) {
                istmt.run(parse(line.toString())[0]);
            }
        }
    });
};



Dbase.prototype.new = function (tname, iterator, pkeys) {
    var self = this;
    self.drop(tname);
    var firstItem = iterator.next();
    _withTrans(self.db, function () {
        if (!firstItem.done) {
            var r = firstItem.value;
            self.db.prepare(_cstmt(tname, Object.keys(r), pkeys)).run();
            var istmt = self.db.prepare(_istmt(tname, Object.keys(r).length));
            istmt.run(Object.values(r));
            for (let r1 of iterator) {
                istmt.run(Object.values(r1));
            }
        }
    });
};



Dbase.prototype.getTables = function () {
    var q = this.db.prepare(
        "select * from sqlite_master where type='table'"
    );
    var result = []
    for (var x of q.iterate()) {
        result.push(x.tbl_name)
    }
    return result;
};


Dbase.prototype.toCsv = function (tname, options) {
    var self = this;
    var fname = path.join(CWD, tname + '.csv');
    if (fs.existsSync(fname)) {
        fs.unlinkSync(fname);
    }
    var fd = fs.openSync(fname, 'a');
    var seq = self.fetch(tname, options);
    var firstElt = seq.next().value;
    fs.writeSync(fd, Object.keys(firstElt).join(',') + '\n');
    fs.writeSync(fd, Object.values(firstElt).join(',') + '\n');
    for (let r of seq) {
        fs.writeSync(fd, Object.values(r).join(',') + '\n');
    }
    fs.closeSync(fd);
};


Dbase.prototype.fetch = function* (tname, options) {
    var _roll = function* (seq, size, step) {
        var ss = it.tee(seq, size);
        for (var [cnt, s1] of ss.entries()) {
            for (var i of it.range(cnt)) {
                s1.next()
            }
        }
        var xss = it.zipLongest.apply(null, [null].concat(ss));

        for (var xs of it.slice({
                step: step
            }, xss)) {
            yield xs.filter((x) => x !== null);
        }
    };

    options = options || {};
    var columns = options.columns || [];
    var where = options.where || "";
    var order = options.orderBy || [];
    var group = options.groupBy || [];
    var overlap = options.overlap;
    // use spare db
    var db = this.db2;

    if (!_isFalsy(group)) {
        order = _listify(group).concat(_listify(order));
    }

    var rows = db.prepare(_buildQuery(tname, columns, where, order)).iterate();
    if (!_isFalsy(overlap)) {
        // default step size is 1
        var [size, step] = Array.isArray(overlap) ? overlap : [overlap, 1];

        if (!_isFalsy(group)) {
            var rows = it.map((x) => _arr(x[1]), it.groupby(rows, _buildKeyfn(group)));
        }

        for (var x of _roll(rows, size, step)) {
            // pretty expensive, but safety is much more important!!
            yield copy(_.flatten(x));
        }
    } else if (!_isFalsy(group)) {
        for (var x of it.groupby(rows, _buildKeyfn(group))) {
            yield _arr(x[1]);
        }
    } else {
        for (var x of rows) {
            yield x;
        }
    }
};


Dbase.prototype.drop = function (tnames) {
    var self = this;
    _withTrans(self.db, function () {
        for (tname of _listify(tnames)) {
            self.db.prepare("drop table if exists " + tname).run();
        }
    });
};


Dbase.prototype.join = function (newName, tinfos, pkeys) {
    // parse tinfos
    var tinfos1 = [];
    for (let [tname, cols, mcols] of tinfos) {
        tinfos1.push([tname,
            _listify(cols).map((c) => tname + '.' + c),
            _listify(mcols)
        ]);
    }

    var tname0 = tinfos1[0][0];
    var mcols0 = tinfos1[0][2];

    var joinClauses = [];
    var eqs;
    for (let [tname, cols, mcols] of tinfos1.slice(1)) {
        eqs = [];
        for (let i = 0; i < mcols0.length; i++) {
            eqs.push(`${tname0}.${mcols0[i]} = ${tname}.${mcols[i]}`);
        }
        joinClauses.push(`left join ${tname} on ${eqs.join(" and ")}`);
    }

    var qry = `select ${tinfos1.map((x) => x[1].join(', ')).join(', ')}
    from ${tname0} ${joinClauses.join(' ')}`;
    this.createAs(newName, qry, pkeys);

};


Dbase.prototype.register = function (...args) {
    this.db.register(...args);
    this.db2.register(...args);
}


Dbase.prototype.createAs = function (name, query, pkeys) {
    var self = this;
    var getName = function (query) {
        query1 = query.split(" ").map((x) => x.trim());
        return query1[query1.indexOf("from") + 1];
    };

    name = name || getName(query);
    var temp_name = 'temp_' + Math.random().toString(36).substring(7);

    _withTrans(self.db, function () {
        try {
            self.db.prepare(_cstmt(temp_name, self._getColumns(query), pkeys)).run();
            self.db.prepare(`insert into ${temp_name} ${query}`).run();
            self.db.prepare(`drop table if exists ${name}`).run();
            self.db.prepare(`alter table ${temp_name} rename to ${name}`).run();
        } finally {
            self.db.prepare(`drop table if exists ${temp_name}`).run();
        }
    });
};


Dbase.prototype._getColumns = function (query) {
    return Object.keys(this.db.prepare(query).get());
};


Dbase.prototype._getPrimaryKeys = function (tname) {
    var result = this.db.pragma(`table_info(${tname})`)
        .filter((x) => x.pk !== 0)
        .map((x) => x.name);
    return result.length === 0 ? null : result
}


Dbase.prototype.collect = function (tname, dbs) {
    var self = this;
    var istmt;
    self.drop(tname);
    _withTrans(self.db, function () {
        for (let db of dbs) {
            connect(db, (c) => {
                for (let r of c.fetch(tname)) {
                    if (!istmt) {
                        self.db.prepare(_cstmt(tname, Object.keys(r), c._getPrimaryKeys())).run();
                        istmt = self.db.prepare(_istmt(tname, Object.keys(r).length));
                    }
                    istmt.run(Object.values(r));
                }
            });
        }
    });
};


Dbase.prototype.split = function (tname, dbwheres) {
    var self = this;
    var pkeys = self._getPrimaryKeys(tname);
    for (let db of Object.keys(dbwheres)) {
        connect(db, (c) => {
            c.new(tname, function* () {
                yield* self.fetch(tname, {
                    where: dbwheres[db]
                });
            }(), pkeys);
        });
    }
};


// for parallel works across databases
pconnect = function (dbargs, fn, fnexit, options) {
    var masterProcess = function () {
        var completed_processes = 0;
        for (let db of Object.keys(dbargs)) {
            const worker = cluster.fork();
            worker.send({
                db: db,
                arg: dbargs[db]
            });
            worker.on('message', (message) => {
                completed_processes += 1;
                if (completed_processes === Object.keys(dbargs).length) {
                    fnexit();
                    // process.exit();
                }
            });
        }
    };

    var childProcess = function () {
        process.on('message', (message) => {
            var db = message.db;
            var arg = message.arg;
            connect(db, (c) => {
                fn(c, arg);
            }, options);
            // Whatever is fine
            process.send({
                done: true
            });
            process.exit();
        })
    };

    if (cluster.isMaster) {
        masterProcess();
    } else {
        childProcess();
    }
};


function connect(fname, fn, options) {
    options = options || {};
    var dbase = new Dbase(path.join(CWD, fname));
    var db = dbase.db;
    try {
        db.pragma("journal_mode=OFF");
        // db.pragma('journal_mode = WAL');
        db.pragma("count_changes=" + (options.countChanges || 0));
        db.pragma("temp_store=" + (options.tempStore || 2));
        db.pragma("cache_size=" + (options.cacheSize || 99999));
        // ...
        dbase.register({
            varargs: true
        }, isNum);

        fn(dbase);
    } finally {
        db.close();
    }
};


function _arr(x) {
    var result = [];
    for (let x1 of x) {
        result.push(x1);
    }
    return result;
}


function _listify(x) {
    if (Array.isArray(x)) {
        return x;
    } else {
        return x.split(',').map((a) => a.trim());
    }
}


function _cstmt(tname, cols, pkeys) {
    pkeys = _isFalsy(pkeys) ? [] : [`primary key (${ _listify(pkeys).join(', ') })`];
    cols = _listify(cols).map((c) => c + " numeric")
    var schema = cols.concat(pkeys).join(', ');
    return `create table if not exists ${tname} (${schema})`;
}


function _istmt(tname, n) {
    var qs = []
    for (var i = 0; i < n; i++) {
        qs.push("?");
    }
    return `insert into ${tname} values (${qs.join(", ")})`;
}


function _isFalsy(x) {
    if (_.isNumber(x)) {
        return (x === 0) ? true : false;
    }
    return (x === undefined) || x === null || _.isEmpty(x);
}


function _buildQuery(tname, cols, where, order) {
    cols = _isFalsy(cols) ? '*' : _listify(cols).join(', ');
    where = _isFalsy(where) ? '' : 'where ' + where;
    order = _isFalsy(order) ? '' : `order by ${_listify(order).join(', ')}`;
    return `select ${cols} from ${tname} ${where} ${order}`;
}


function _buildKeyfn(cols) {
    if (typeof cols === "function") {
        return cols;
    } else {
        cols = _listify(cols);
        if (cols.length === 1) {
            col = cols[0];
            return (r) => r[col];
        } else {
            return (r) => cols.map((c) => r[c]);
        }
    }
}


function orderBy(rs, cols) {
    var a1, b1, keyfn = _buildKeyfn(cols);
    rs.sort((a, b) => {
        [a1, b1] = [keyfn(a), keyfn(b)];
        if (a1 === b1) return 0;
        return (a1 < b1) ? -1 : 1;
    });
    return rs;
};


function groupBy(rs, cols) {
    var keyfn = cols ? _buildKeyfn(cols) : (x) => x;
    var curval = keyfn(rs[0]),
        beg = 0,
        n = rs.length;
    var tgtval;

    var result = []
    for (let i = 0; i < n; i++) {
        tgtval = keyfn(rs[i]);
        if (tgtval !== curval) {
            result.push(rs.slice(beg, i));
            beg = i;
            curval = tgtval;
        }
    }
    result.push(rs.slice(beg));
    return result;
}


function overlap(rs, size, step) {
    var r0 = rs[0];

    step = step || 1;
    var n = rs.length;
    var result = [];
    for (let i = 0; i < n; i += step) {
        var rs1 = rs.slice(i, i + size);
        if (Array.isArray(r0)){
            rs1 = _.flatten(rs1);
        }
        // expensive!!
        result.push(copy(rs1));
    }
    return result;
}


function copy(rs) {
    var result = [];
    for (let r of rs) {
        result.push(Object.assign({}, r));
    }
    return result;
}


function setdir(path) {
    CWD = path;
}


function isNum(...xs) {
    return xs.every((n) => !isNaN(parseFloat(n)) && isFinite(n)) ? 1 : 0;
}


function _withTrans(db, thunk) {
    var begin = db.prepare('BEGIN');
    var commit = db.prepare('COMMIT');
    var rollback = db.prepare('ROLLBACK');
    begin.run();
    try {
        thunk();
        commit.run();
    } finally {
        if (db.inTransaction) rollback.run();
    }
};


module.exports = {
    connect,
    pconnect,
    setdir,
    overlap,
    isNum,
    groupBy,
    orderBy
};