const { frombuf, aggregate } = require('.');


describe('check aggregation', () => {
    it('create table', () => {
        const t1x1 = frombuf(['a', '1'], 1);
        const t2x1 = frombuf(['a', '1', '2'], 1);
        const t1x2 = frombuf(['a', 'b', '1', '2'], 2);
        const t2x2 = frombuf(['a', 'b', '1', '2', '3', '4'], 2);
        expect(t1x1.headers.names).toEqual(['a']);
        expect(t2x1.headers.names).toEqual(['a']);
        expect(t1x2.headers.names).toEqual(['a', 'b']);
        expect(t2x2.headers.names).toEqual(['a', 'b']);
        expect(t1x1.headers.cols).toEqual({ a: 0 });
        expect(t2x1.headers.cols).toEqual({ a: 0 });
        expect(t1x2.headers.cols).toEqual({ a: 0, b: 1 });
        expect(t2x2.headers.cols).toEqual({ a: 0, b: 1 });
        expect(t1x1.distinct).toEqual({ a: { '1': 1 } });
        expect(t2x1.distinct).toEqual({ a: { '1': 1, '2': 1 } });
        expect(t1x2.distinct).toEqual({ a: { '1': 1 }, b: { '2': 1 } });
        expect(t2x2.distinct).toEqual({ a: { '1': 1, '3': 1 }, b: { '2': 1, '4': 1 } });
        expect(t1x1.table).toEqual(['1']);
        expect(t2x1.table).toEqual(['1', '2']);
        expect(t1x2.table).toEqual(['1', '2']);
        expect(t2x2.table).toEqual(['1', '2', '3', '4']);
        expect(t1x1.rows).toEqual(1);
        expect(t2x1.rows).toEqual(2);
        expect(t1x2.rows).toEqual(1);
        expect(t2x2.rows).toEqual(2);
    });
    it('aggregate table', () => {
        const t5x5 = frombuf([
            'a', 'b', 'c', 'd', 'e',
            '1', '2', '3', '4', '5',
            '6', '7', '8', '9', '10',
            '11', '12', '13', '14', '15',
            '11', '7', '3', '14', '2',
            '6', '7', '8', '4', '4',
        ], 5);

        const abc = aggregate(t5x5, {
            rows: ['a'],
            cols: ['b'],
            sums: ['c'],
        });
        expect(abc.rows).toEqual([[{ name: 'a', value: '1' }], [{ name: 'a', value: '6' }], [{ name: 'a', value: '11' }]]);
        expect(abc.cols).toEqual([[{ name: 'b', value: '2' }], [{ name: 'b', value: '7' }], [{ name: 'b', value: '12' }]]);
        expect(abc.sums).toEqual(['c']);
        expect(abc.table['1&2&c']).toEqual(3);
        expect(abc.table['6&7&c']).toEqual(16);
        expect(abc.table['*&7&c']).toEqual(19);
        expect(abc.table['1&*&c']).toEqual(3);
        expect(abc.table['*&*&c']).toEqual(35);


        const abcde = aggregate(t5x5, {
            rows: ['a', 'b'],
            cols: ['c', 'd'],
            sums: ['e'],
        });
        expect(abcde.rows).toEqual([
            [{ name: 'a', value: '1' }, { name: 'b', value: '2' }],
            [{ name: 'a', value: '1' }, { name: 'b', value: '7' }],
            [{ name: 'a', value: '1' }, { name: 'b', value: '12' }],
            [{ name: 'a', value: '6' }, { name: 'b', value: '2' }],
            [{ name: 'a', value: '6' }, { name: 'b', value: '7' }],
            [{ name: 'a', value: '6' }, { name: 'b', value: '12' }],
            [{ name: 'a', value: '11' }, { name: 'b', value: '2' }],
            [{ name: 'a', value: '11' }, { name: 'b', value: '7' }],
            [{ name: 'a', value: '11' }, { name: 'b', value: '12' }],
        ]);
        expect(abcde.cols).toEqual([
            [{ name: 'c', value: '3' }, { name: 'd', value: '4' }],
            [{ name: 'c', value: '3' }, { name: 'd', value: '9' }],
            [{ name: 'c', value: '3' }, { name: 'd', value: '14' }],
            [{ name: 'c', value: '8' }, { name: 'd', value: '4' }],
            [{ name: 'c', value: '8' }, { name: 'd', value: '9' }],
            [{ name: 'c', value: '8' }, { name: 'd', value: '14' }],
            [{ name: 'c', value: '13' }, { name: 'd', value: '4' }],
            [{ name: 'c', value: '13' }, { name: 'd', value: '9' }],
            [{ name: 'c', value: '13' }, { name: 'd', value: '14' }],
        ]);
        expect(abcde.sums).toEqual(['e']);
        expect(abcde.table['6/12&8/4&e']).toEqual(0);
        expect(abcde.table['*&3/4&e']).toEqual(5);
        expect(abcde.table['11/12&*&e']).toEqual(15);
        expect(abcde.table['*&*&e']).toEqual(36);
    });

    it('aggregate vector', () => {
        const t2x5 = frombuf([
            'a', 'b',
            '1', '2',
            '2', '1',
            '3', '0',
            '4', '-1',
            '5', '-2',
        ], 2);

        const ab = aggregate(t2x5, {
            rows: ['a'],
            cols: [],
            sums: ['b'],
        });
        expect(ab.rows).toEqual([
            [{ name: 'a', value: '1' }],
            [{ name: 'a', value: '2' }],
            [{ name: 'a', value: '3' }],
            [{ name: 'a', value: '4' }],
            [{ name: 'a', value: '5' }],
        ]);
        expect(ab.cols).toEqual([]);
        expect(ab.sums).toEqual(['b']);
        expect(ab.table['1&*&b']).toEqual(2);
        expect(ab.table['3&*&b']).toEqual(0);
        expect(ab.table['5&*&b']).toEqual(-2);
        expect(ab.table['*&*&b']).toEqual(0);

        const ba = aggregate(t2x5, {
            rows: [],
            cols: ['a'],
            sums: ['b'],
        });
        expect(ba.rows).toEqual([]);
        expect(ba.cols).toEqual([
            [{ name: 'a', value: '1' }],
            [{ name: 'a', value: '2' }],
            [{ name: 'a', value: '3' }],
            [{ name: 'a', value: '4' }],
            [{ name: 'a', value: '5' }],
        ]);
        expect(ba.sums).toEqual(['b']);
        expect(ba.table['*&1&b']).toEqual(2);
        expect(ba.table['*&3&b']).toEqual(0);
        expect(ba.table['*&5&b']).toEqual(-2);
        expect(ba.table['*&*&b']).toEqual(0);
    });
});