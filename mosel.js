var fs = require('fs');
const it = require('iter-tools');
var path = require('path');
var parse = require('csv-parse/lib/sync');
var Database = require('better-sqlite3');
var lineByLine = require('n-readlines');
var cluster = require('cluster');
const ncores = require('physical-cpu-count');


// current working directory
var CWD = '';


function Dbase(dbfile) {
    this.db = new Database(dbfile);
    // another connection, for fetching, read only
    this.rodb = new Database(dbfile, {
        readonly: true
    });
}


Dbase.prototype.load = function (fname, tname, pkeys) {
    var cnt = 0;
    var istmt = false;
    var db = this.db;
    tname = tname || fname.split('.')[0];
    if (this.getTables().indexOf(tname) === -1) {
        var liner = new lineByLine(path.join(CWD, fname));
        var firstLine = parse(liner.next().toString())[0];
        db.prepare(_cstmt(tname, firstLine, pkeys)).run();
        var istmt = db.prepare(_istmt(tname, firstLine.length));
        var line;
        while (line = liner.next()) {
            istmt.run(parse(line.toString())[0]);
        }
    }
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


Dbase.prototype.prepare = function (row, tname, pkeys) {
    this.drop(tname);
    this.db.prepare(_cstmt(tname, Object.keys(row), pkeys)).run();
    return this.db.prepare(_istmt(tname, Object.keys(row).length));
}


Dbase.prototype.toCSV = function (tname, options) {
    var stream = fs.createWriteStream(path.join(CWD, tname + '.csv'));
    for (let row of this.db.fetch(tname, options)) {
        stream.write(Object.values(row).join(',') + '\n');
    }
    stream.end();
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
    var columns = options.columns || "";
    var where = options.where || "";
    var order = options.orderBy || "";
    var group = options.groupBy || "";
    var overlap = options.overlap;
    // use readonly database
    var db = this.rodb;

    if (group) {
        order = _listify(group).concat(_listify(order));
    }

    var rows = db.prepare(_buildQuery(tname, columns, where, order)).iterate();
    if (group) {
        for (var x of it.groupby(rows, _buildKeyfn(group))) {
            yield _arr(x[1]);
        }
    } else if (overlap) {
        // default step size is 1
        var [size, step] = Array.isArray(overlap) ? overlap : [overlap, 1];
        var grows = it.map((x) => x[1], it.groupby(rows, _buildKeyfn(group)));
        for (var x of _roll(grows, size, step)) {
            yield _arr(it.chain.apply(null, x));
        }

    } else {
        for (var x of rows) {
            yield x;
        }
    }
};


Dbase.prototype.drop = function (tnames) {
    for (tname of _listify(tnames)) {
        this.db.prepare("drop table if exists " + tname).run();
    }
};


Dbase.prototype.join = function (tinfos, newName, pkeys) {
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
            eqs.push(`${tname}.${mcols0[i]} = ${tname}.${mcols[i]}`);
        }
        joinClauses.push(`left join ${tname} on ${eqs.join(" and ")}`);
    }

    var qry = `select ${tinfos1.map((x) => x[1].join(', ')).join(', ')}
    from ${tname0} ${joinClauses.join(' ')}`;
    this.createAs(qry, newName, pkeys);
};


Dbase.prototype.register = function (...args) {
    this.db.register(...args);
    this.rodb.register(...args);
}


Dbase.prototype.createAs = function (query, name, pkeys) {
    var getName = function (query) {
        query1 = query.split(" ").map((x) => x.trim());
        return query1[query1.indexOf("from") + 1];
    };
    name = name || getName(query);
    var temp_name = 'temp_' + Math.random().toString(36).substring(7);
    try {
        this.db.prepare(_cstmt(temp_name, this._getColumns(query), pkeys)).run();
        this.db.prepare(`insert into ${temp_name} ${query}`).run();
        this.db.prepare(`drop table if exists ${name}`).run();
        this.db.prepare(`alter table ${temp_name} rename to ${name}`).run();
    } finally {
        this.db.prepare(`drop table if exists ${temp_name}`).run();
    }
};


Dbase.prototype._getColumns = function (query) {
    return Object.keys(this.db.prepare(query).get());
};


Dbase.prototype._getPrimaryKeys = function (tname) {
    var result =  this.db.pragma(`table_info(${tname})`)
        .filter((x) => x.pk !== 0)
        .map((x) => x.name);
    return result.length === 0 ? null : result
}


Dbase.prototype.collect = function (dbs, tname) {
    var self = this;
    var istmt;
    for (let db of dbs) {
        connect(db, (c) => {
            for (var r of c.fetch(tname)) {
                if (!istmt) {
                    istmt = self.prepare(r, tname, c._getPrimaryKeys(tname));
                }
                istmt.run(Object.values(r));
            }
        });
    }
};


Dbase.prototype.split = function (tname, dbwheres) {
    var self = this;
    var pkeys = self._getPrimaryKeys(tname);
    for (let db of Object.keys(dbwheres)) {
        var where = dbwheres[db];
        connect(db, (c) => {
            var istmt;
            for (let r of self.fetch(tname, {
                    where: where
                })) {
                if (!istmt) {
                    istmt = c.prepare(r, tname, pkeys);
                }
                istmt.run(Object.values(r));
            }
        });
    }
};


// for parallel works across databases
pconnect = function (dbargs, fn, options) {
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
                if (completed_processes === dbargs.length) {
                    process.exit();
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


function connect(fnames, fn, options) {
    if (!Array.isArray(fnames)) {
        fnames = [fnames];
    }
    options = options || {};
    var ssqls = fnames.map((fname) => new Dbase(path.join(CWD, fname)));
    try {
        ssqls.forEach((ssql) => {
            ssql.db.pragma("journal_mode=OFF");
            // db.pragma('journal_mode = WAL');
            ssql.db.pragma("count_changes=" + (options.countChanges || 0));
            ssql.db.pragma("temp_store=" + (options.tempStore || 2));
            ssql.db.pragma("cache_size=" + (options.cacheSize || 99999));
            // ...
            ssql.register({
                varargs: true
            }, isNum);
            ssql.db.prepare('BEGIN').run();
            // not sure if transaction prevents locks
            ssql.rodb.prepare('BEGIN').run();
        });

        fn(...ssqls);

        ssqls.forEach((ssql) => {
            ssql.rodb.prepare('END TRANSACTION').run();
            ssql.db.prepare('COMMIT').run();
        });

    } finally {
        ssqls.forEach((ssql) => {
            if (ssql.db.inTransaction) {
                ssql.rodb.prepare('END TRANSACTION').run();
                ssql.db.prepare('ROLLBACK').run();
            }
            ssql.db.close();
            // close the read only db as well
            ssql.rodb.close();
        });
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
    pkeys = pkeys ? [`primary key (${ _listify(pkeys).join(', ') })`] : []
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


function _buildQuery(tname, cols, where, order) {
    cols = (cols.length > 0) ? _listify(cols).join(', ') : '*';
    where = (where.trim().length > 0) ? 'where ' + where : '';
    order = (order.length > 0) ? 'order by ' + _listify(order).join(', ') : '';
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
    return rs
};


function* groupBy(rs, cols) {
    var keyfn = cols ? _buildKeyfn(cols) : (x) => x;
    var curval = keyfn(rs[0]),
        beg = 0,
        n = rs.length;
    var tgtval;

    for (let i = 0; i < n; i++) {
        tgtval = keyfn(rs[i]);
        if (tgtval !== curval) {
            yield rs.slice(beg, i);
            beg = i;
            curval = tgtval;
        }
    }
    yield rs.slice(beg);
}


function* overlap(rs, size, step) {
    step = step || 1;
    var n = rs.length;
    for (let i = 0; i < n; i += step) {
        yield rs.slice(i, i + size);
    }
}


function setdir(path) {
    CWD = path;
}


function isNum(...xs) {
    return xs.every((n) => !isNaN(parseFloat(n)) && isFinite(n)) ? 1 : 0;
}


module.exports = {
    connect,
    pconnect,
    setdir,
    overlap,
    isNum,
    groupBy,
    orderBy
};