import { seq } from 'incnum';

export type Table = {
    /**
     * header names in this table
     * 
     * 'frombuf' methods create headers from arg of 'buf' as headers count.
     * 
     * @see frombuf
     */
    headers: {
        /**
         * name of headers as is in table.
         */
        names: string[];
        /**
         * maps column index from header name
         */
        cols: { [name in string]: number };
    };
    /**
     * distincted values in table by header names.
     * 
     * for example, if table had column of '1990', '1991', '1991' then this is distinct['year'] = { '1990': 1, '1991': 1 }.
     * 
     * as a result, you can check exists value in table by if(distinct['header']['value'])
     */
    distinct: { [header in string]: { [value in string]: number } };
    /**
     * the origin table does not contain header.
     * 
     * starts with body part.
     */
    table: string[];
    /**
     * count of rows.
     * 
     * if the 'buf' cloud not divide with headers count, rows is unspecified.
     */
    rows: number;
};

/**
 * create table from raw buffer.
 * 
 * buf should be all of;
 * - header-part: contains header names from first elements to 'headers' count
 * - body-part: length should be N * headers count
 * - and N will be rows of table
 */
export function frombuf(buf: string[], headers: number): Table {
    const r: Table = {
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

    for (const [header, row] of seq([0, 0], [r.headers.names.length, r.rows])) {
        const name = r.headers.names[header];
        const col = r.headers.cols[name];
        const value = r.table[r.headers.names.length * row + col];
        r.distinct[name][value] = 1;
    }

    return r;
}

export type Aggregating<T = number> = {
    /**
     * names of row in table header
     */
    rows: string[];
    /**
     * names of col in table header
     */
    cols: string[];
    /**
     * names of aggregation in table header
     */
    sums: string[];
    /**
     * aggregation template methods.
     * 
     * if you omitted this, default aggregator works.
     * 
     * you must specify all methods in this object if aggregator type was different with the 'number'.
     * 
     * also,  you can omit these if aggregator was 'number'
     * 
     * @see Aggregator
     */
    agg?: Aggregator<T>;
};
export type Aggregator<T> = {
    /**
     * fetch data point from table for aggregation.
     * 
     * default casts;
     * - into the 'number' type if not nan
     * - '0' if nan
     */
    cast?: (value: string) => T;
    /**
     * add and aggregate point data into sum value
     * 
     * default adds point as number 
     */
    add?: (prev: T, current: T, row: AggItem[], col: AggItem[]) => T;
    /**
     *  put sum value into result table if this returned true
     * 
     * default puts all sum values
     */
    postif?: (sum: T, row: AggItem[], col: AggItem[]) => any;
    /**
     * put sum value into result table.
     * 
     * the sum value related with 'rcid' in aggregated result table.
     * 
     * default puts as is
     */
    post?: (sum: T) => T;
    /**
     * aggregate and generate row-id, col-id
     * 
     * the 'rcid' will consist with ${row-id}&${col-id}&${sum-id} in default.
     * 
     * default generates string consist with '/' separated values.
     */
    keys?: (prev: string, current: string) => string;
    /**
     * wild card for row-id like rcid = ${row-id}&*&${sum-id}
     * 
     * default is '*'
     */
    wkey?: string;
    /**
     * generate the 'rcid' from row-id, col-id, sum-id.
     * 
     * these ids is header name, and the 'rcid' will be used in aggregated table key.
     * 
     * thus, for example, table['1990/1&us/alice&sales'] can be accessed.
     * 
     * default generates amp '&' separated values
     */
    rcid?: (row: string, col: string, sum: string) => string;
    /**
     * returns of start zero value of sum value.
     * 
     * all aggregation and sumation starts with this value. 
     * 
     * default returns zero as number type.
     */
    zero?: T;
};

export type Aggregated<T = number> = {
    /**
     * the rows distincted and emunerated table headers and values for pivot table .
     * 
     * so, for example, if you configured rows for 'year', 'month', this will be;
     * - [{name: 'year', value: '1990'}, {name: 'month', value: '1'}],
     * - [{name: 'year', value: '1990'}, {name: 'month', value: '2'}],
     * - [{name: 'year', value: '1990'}, {name: 'month', value: '3'}],
     * - ...
     * - [{name: 'year', value: '1991'}, {name: 'month', value: '1'}],
     * - ...
     */
    rows: AggItem[][];
    /**
     * @see rows
     */
    cols: AggItem[][];
    /**
     * the sum-ids you configured.
     */
    sums: string[];
    /**
     * the simple pivot table by mapping the 'rcid'.
     * 
     * 'rcid' will be ${row-id}&${col-id}&${sum-id} if you omitted aggregator option.
     */
    table: { [rcid in string]: T }
};
export type AggItem = {
    name: string;
    value: string;
};

export function aggregate<T = number>(table: Table, agg: Aggregating<T>) {
    const r: Aggregated<T> = {
        rows: [],
        cols: [],
        sums: agg.sums.concat(),
        table: {},
    };
    const distinct = {
        rows: agg.rows.map(header => Object.keys(table.distinct[header])),
        cols: agg.cols.map(header => Object.keys(table.distinct[header])),
    };

    seq(agg.rows.map(() => 0), agg.rows.map((x, p) => distinct.rows[p].length)).forEach(values => {
        r.rows.push(values.map((value, name) => ({
            value: distinct.rows[name][value],
            name: agg.rows[name],
        })));
    });
    seq(agg.cols.map(() => 0), agg.cols.map((x, p) => distinct.cols[p].length)).forEach(values => {
        r.cols.push(values.map((value, name) => ({
            value: distinct.cols[name][value],
            name: agg.cols[name],
        })));
    });


    const ag: AggregatorInternal<T> = Object.assign({}, defagg, agg.agg)
    const keys = {
        rows: r.rows.map(x => x.map(({ value }) => value).reduce(ag.keys)),
        cols: r.cols.map(x => x.map(({ value }) => value).reduce(ag.keys)),
        sums: r.sums.reduce(ag.keys)
    };

    r.rows.map((row, rp) => r.cols.map((col, cp) => r.sums.map(sum => {
        const rcid = ag.rcid(keys.rows[rp], keys.cols[cp], sum);
        let a = ag.zero;
        for (let t = 0; t < table.rows; t++) {
            const match = [...row, ...col].every(({ name, value }) => table.table[table.headers.names.length * t + table.headers.cols[name]] === value);
            if (match) {
                const current = ag.cast(table.table[table.headers.names.length * t + table.headers.cols[sum]]);
                a = ag.add(a, current, row, col);
            }
        }
        if (ag.postif(a, row, col)) {
            r.table[rcid] = ag.post(a);
        }
    })));

    const wildcard: AggItem[] = [{ name: ag.wkey, value: ag.wkey }];

    r.rows.map((row, rp) => r.sums.map(sum => {
        const rcid = ag.rcid(keys.rows[rp], ag.wkey, sum);
        let a = ag.zero;
        for (let t = 0; t < table.rows; t++) {
            const match = row.every(({ name, value }) => table.table[table.headers.names.length * t + table.headers.cols[name]] === value);
            if (match) {
                const current = ag.cast(table.table[table.headers.names.length * t + table.headers.cols[sum]]);
                a = ag.add(a, current, row, wildcard);
            }
            if (ag.postif(a, row, wildcard)) {
                r.table[rcid] = ag.post(a);
            }
        }
    }));

    r.cols.map((col, cp) => r.sums.map(sum => {
        const rcid = ag.rcid(ag.wkey, keys.cols[cp], sum);
        let a = ag.zero;
        for (let t = 0; t < table.rows; t++) {
            const match = col.every(({ name, value }) => table.table[table.headers.names.length * t + table.headers.cols[name]] === value);
            if (match) {
                const current = ag.cast(table.table[table.headers.names.length * t + table.headers.cols[sum]]);
                a = ag.add(a, current, wildcard, col);
            }
            if (ag.postif(a, wildcard, col)) {
                r.table[rcid] = ag.post(a);
            }
        }
    }));

    r.sums.map(sum => {
        const rcid = ag.rcid(ag.wkey, ag.wkey, sum);
        let a = ag.zero;
        for (let t = 0; t < table.rows; t++) {
            const current = ag.cast(table.table[table.headers.names.length * t + table.headers.cols[sum]]);
            a = ag.add(a, current, wildcard, wildcard);
        }
        if (ag.postif(a, wildcard, wildcard)) {
            r.table[rcid] = ag.post(a);
        }
    });

    return r;
}


type AggregatorInternal<T> = {
    readonly cast: (value: string) => T;
    readonly add: (prev: T, current: T, row: AggItem[], col: AggItem[]) => T;
    readonly postif: (sum: T, row: AggItem[], col: AggItem[]) => any;
    readonly post: (sum: T) => T;
    readonly keys: (prev: string, current: string) => string;
    readonly wkey: string;
    readonly rcid: (row: string, col: string, sum: string) => string;
    readonly zero: T;
};
export const defagg: AggregatorInternal<any/*of number*/> = {
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

