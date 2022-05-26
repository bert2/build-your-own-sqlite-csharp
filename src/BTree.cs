﻿namespace codecrafters_sqlite;

using System.Collections.Generic;
using System.Linq;
using Nullable.Extensions;

using static System.Diagnostics.Debug;

public static class BTree {
    public static LeafTblCell? IntPkScan(long rowId, Page page, Db db) {
        var type = page.Header.PageType;

        if (type == BTreePage.LeafTbl) {
            return page
                .CellPtrs()
                .Select(ptr => LeafTblCell.Parse(page.Data[ptr..]))
                .FirstOrDefault(cell => cell.RowId == rowId);
        }

        Assert(type is BTreePage.IntrTbl, $"Cannot search cells by integer primary key in {type}.");

        return page
            .CellPtrs()
            .Select(ptr => IntrTblCell.Parse(page.Data[ptr..]))
            .FirstOrDefault(cell => rowId <= cell.RowId)
            .Switch(
                notNull: cell => IntPkScan(rowId, Page.Parse(cell.ChildPage, db), db),
                isNull: () => {
                    var rightMostChildPage = page.Header.RightMostPtr
                        ?? throw new InvalidOperationException($"Expected {type} to have right most child page pointer.");
                    return IntPkScan(rowId, Page.Parse(rightMostChildPage, db), db);
                });
    }

    public static IEnumerable<LeafTblCell> IdxScan(string key, Page idxPage, Page tblPage, Db db) {
        return FindRowIds(idxPage).Select(rowId =>
            IntPkScan(rowId, tblPage, db) ?? throw new InvalidOperationException($"Row ID {rowId} referenced by index not found in table."));

        IEnumerable<long> FindRowIds(Page page) {
            var type = page.Header.PageType;

            if (type == BTreePage.LeafIdx) {
                return page
                    .CellPtrs()
                    .Select(ptr => LeafIdxCell.Parse(page.Data[ptr..]))
                    .SkipWhile(cell => cell.Payload[0].ToUtf8String().LessThan(key))
                    .TakeWhile(cell => cell.Payload[0].ToUtf8String() == key)
                    .Select(cell => cell.Payload[1].ToLong());
            }

            Assert(type is BTreePage.IntrIdx, $"Cannot search cells by index in {type}.");

            var rightMostChildPage = page.Header.RightMostPtr
                ?? throw new InvalidOperationException($"Expected {type} to have right most child page pointer.");

            var intrRowIds = new LinkedList<long>();

            return page
                .CellPtrs()
                .Select(ptr => IntrIdxCell.Parse(page.Data[ptr..]))
                .Where(cell => key.LessOrEqualThan(cell.Payload[0].ToUtf8String()))
                .Select(cell => {
                    if (cell.Payload[0].ToUtf8String() == key)
                        _ = intrRowIds.AddLast(cell.Payload[1].ToLong());
                    return cell;
                })
                .Select(cell => Page.Parse(cell.ChildPage, db))
                .Append(() => Page.Parse(rightMostChildPage, db))
                .SelectMany(FindRowIds)
                .Concat(intrRowIds);
        }
    }

    public static IEnumerable<LeafTblCell> TblScan(Page page, Db db) {
        return LeafPages(page)
            .SelectMany(pg => pg.CellPtrs(), (pg, ptr) => (pg, ptr))
            .Select(x => LeafTblCell.Parse(x.pg.Data[x.ptr..]));

        IEnumerable<Page> LeafPages(Page page) {
            var type = page.Header.PageType;

            if (type == BTreePage.LeafTbl) return page.Yield();

            Assert(type is BTreePage.IntrTbl, $"Cannot get leaf pages of {type}.");

            var rightMostChildPage = page.Header.RightMostPtr
                ?? throw new InvalidOperationException($"Expected {type} to have right most child page pointer.");

            return page
                .CellPtrs()
                .Select(ptr => IntrTblCell.Parse(page.Data[ptr..]))
                .Select(cell => Page.Parse(cell.ChildPage, db))
                .Append(() => Page.Parse(rightMostChildPage, db))
                .SelectMany(LeafPages);
        }
    }
}
