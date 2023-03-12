"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.defagg = exports.aggregate = exports.frombuf = void 0;
const incnum_1 = require("incnum");
const ary_index_1 = require("ary-index");
/**
 * create table from raw buffer.
 *
 * buf should be all of;
 * - header-part: contains header names from first elements to 'headers' count
 * - body-part: length should be N * headers count
 * - and N will be rows of table
 */
function frombuf(buf, headers) {
    const r = {
        headers: {
            names: buf.slice(0, headers),
            cols: {},
        },
        distinct: {},
        table: buf.slice(headers),
        rows: (buf.length - headers) / headers,
    };
    r.headers.names.forEach((name, p) => {
        r.headers.cols[name] = p;
        r.distinct[name] = {};
    });
    const tableptr = (0, ary_index_1.fromary)(r.table, [r.rows, r.headers.names.length]);
    for (const [header, row] of (0, incnum_1.seq)([0, 0], [r.headers.names.length, r.rows])) {
        const name = r.headers.names[header];
        const col = r.headers.cols[name];
        const value = tableptr.get(row, col);
        r.distinct[name][value] = 1;
    }
    return r;
}
exports.frombuf = frombuf;
function aggregate(table, agg) {
    const r = {
        rows: [],
        cols: [],
        sums: agg.sums.concat(),
        table: {},
    };
    const distinct = {
        rows: agg.rows.map(header => Object.keys(table.distinct[header])),
        cols: agg.cols.map(header => Object.keys(table.distinct[header])),
    };
    const tableptr = (0, ary_index_1.fromary)(table.table, [table.rows, table.headers.names.length]);
    (0, incnum_1.seq)(agg.rows.map(() => 0), agg.rows.map((x, p) => distinct.rows[p].length)).forEach(values => {
        r.rows.push(values.map((value, name) => ({
            value: distinct.rows[name][value],
            name: agg.rows[name],
        })));
    });
    (0, incnum_1.seq)(agg.cols.map(() => 0), agg.cols.map((x, p) => distinct.cols[p].length)).forEach(values => {
        r.cols.push(values.map((value, name) => ({
            value: distinct.cols[name][value],
            name: agg.cols[name],
        })));
    });
    const ag = Object.assign({}, exports.defagg, agg.agg);
    const keys = {
        rows: agg.rows.length ? r.rows.map(x => x.map(({ value }) => value).reduce(ag.keys)) : [],
        cols: agg.cols.length ? r.cols.map(x => x.map(({ value }) => value).reduce(ag.keys)) : [],
        sums: r.sums.reduce(ag.keys)
    };
    r.rows.map((row, rp) => r.cols.map((col, cp) => r.sums.map(sum => {
        const rcid = ag.rcid(keys.rows[rp], keys.cols[cp], sum);
        if (!ag.postif(row, col)) {
            return;
        }
        let a = ag.zero;
        for (let t = 0; t < table.rows; t++) {
            const match = [...row, ...col].every(({ name, value }) => tableptr.get(t, table.headers.cols[name]) === value);
            if (match) {
                const current = ag.cast(tableptr.get(t, table.headers.cols[sum]));
                a = ag.add(a, current, row, col);
            }
        }
        r.table[rcid] = ag.post(a);
    })));
    const wildcard = [{ name: ag.wkey, value: ag.wkey }];
    r.rows.map((row, rp) => r.sums.map(sum => {
        const rcid = ag.rcid(keys.rows[rp], ag.wkey, sum);
        if (!ag.postif(row, wildcard)) {
            return;
        }
        let a = ag.zero;
        for (let t = 0; t < table.rows; t++) {
            const match = row.every(({ name, value }) => tableptr.get(t, table.headers.cols[name]) === value);
            if (match) {
                const current = ag.cast(tableptr.get(t, table.headers.cols[sum]));
                a = ag.add(a, current, row, wildcard);
            }
        }
        r.table[rcid] = ag.post(a);
    }));
    r.cols.map((col, cp) => r.sums.map(sum => {
        const rcid = ag.rcid(ag.wkey, keys.cols[cp], sum);
        if (!ag.postif(wildcard, col)) {
            return;
        }
        let a = ag.zero;
        for (let t = 0; t < table.rows; t++) {
            const match = col.every(({ name, value }) => tableptr.get(t, table.headers.cols[name]) === value);
            if (match) {
                const current = ag.cast(tableptr.get(t, table.headers.cols[sum]));
                a = ag.add(a, current, wildcard, col);
            }
        }
        r.table[rcid] = ag.post(a);
    }));
    r.sums.map(sum => {
        const rcid = ag.rcid(ag.wkey, ag.wkey, sum);
        if (!ag.postif(wildcard, wildcard)) {
            return;
        }
        let a = ag.zero;
        for (let t = 0; t < table.rows; t++) {
            const current = ag.cast(tableptr.get(t, table.headers.cols[sum]));
            a = ag.add(a, current, wildcard, wildcard);
        }
        r.table[rcid] = ag.post(a);
    });
    return r;
}
exports.aggregate = aggregate;
exports.defagg = {
    add: (p, c) => p + c,
    keys: (p, c) => `${p}/${c}`,
    wkey: '*',
    rcid: (row, col, sum) => `${row}&${col}&${sum}`,
    zero: 0,
    cast: x => {
        const r = Number(x);
        return isNaN(r) ? 0 : r;
    },
    post: x => x,
    postif: () => true,
};
