namespace codecrafters_sqlite;

using System.Collections.Generic;
using System.Linq;
using Nullable.Extensions;

public static class BTree {
    public static LeafTblCell? IntPkScan(long rowId, Page page, Db db) {
        var type = page.Header.PageType;

        if (type == BTreePage.LeafTbl) {
            return page
                .CellPtrs()
                .Select(ptr => LeafTblCell.Parse(page.Data[ptr..]))
                .FirstOrDefault(cell => cell.RowId == rowId);
        }

        if (type != BTreePage.IntrTbl)
            throw new InvalidOperationException($"Cannot search cells by integer primary key in {type}.");

        var intrCell = page
            .CellPtrs()
            .Select(ptr => IntrTblCell.Parse(page.Data[ptr..]))
            .FirstOrDefault(cell => rowId <= cell.RowId);

        if (intrCell != null)
            return IntPkScan(rowId, Page.Parse(intrCell.ChildPage, db), db);

        var rightMostChildPage = page.Header.RightMostPtr
            ?? throw new InvalidOperationException($"Expected {type} to have right most child page pointer.");

        return IntPkScan(rowId, Page.Parse(rightMostChildPage, db), db);
    }

    public static IEnumerable<LeafTblCell> IdxScan(string key, Page idxPage, Page tblPage, Db db) {
        return FindIdxCells(idxPage)
            .Select(cell => cell.Payload[1].ToLong())
            .Select(rowId => IntPkScan(rowId, tblPage, db)
                ?? throw new InvalidOperationException($"Row ID {rowId} referenced by index not found in table."));

        IEnumerable<LeafIdxCell> FindIdxCells(Page page) {
            var type = page.Header.PageType;

            if (type == BTreePage.LeafIdx) {
                return page
                    .CellPtrs()
                    .Select(ptr => LeafIdxCell.Parse(page.Data[ptr..]))
                    .SkipWhile(cell => cell.Payload[0].ToUtf8String().LessThan(key))
                    .TakeWhile(cell => cell.Payload[0].ToUtf8String() == key);
            }

            if (type != BTreePage.IntrIdx)
                throw new InvalidOperationException($"Cannot search cells by index in {type}.");

            var rightMostChildPage = page.Header.RightMostPtr
                ?? throw new InvalidOperationException($"Expected {type} to have right most child page pointer.");

            return page
                .CellPtrs()
                .Select(ptr => IntrIdxCell.Parse(page.Data[ptr..]))
                .Where(cell => key.LessOrEqualThan(cell.Payload[0].ToUtf8String()))
                .Select(cell => Page.Parse(cell.ChildPage, db))
                .Append(() => Page.Parse(rightMostChildPage, db))
                .SelectMany(FindIdxCells);
        }
    }

    public static IEnumerable<LeafTblCell> TblScan(Page page, Db db) {
        return LeafPages(page)
            .SelectMany(pg => pg.CellPtrs(), (pg, ptr) => (pg, ptr))
            .Select(x => LeafTblCell.Parse(x.pg.Data[x.ptr..]));

        IEnumerable<Page> LeafPages(Page page) {
            var type = page.Header.PageType;
            if (type == BTreePage.LeafTbl)
                return page.Yield();
            if (type != BTreePage.IntrTbl)
                throw new InvalidOperationException($"Cannot get leaf pages of {type}.");
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
