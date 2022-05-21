namespace codecrafters_sqlite;

using System.Collections.Generic;
using System.Linq;

public static class BTree {
    public static IEnumerable<LeafTblCell> FullTblScan(Page page, Db db)
        => LeafPages(page, db)
            .SelectMany(pg => pg.CellPtrs(), (pg, ptr) => (pg, ptr))
            .Select(x => LeafTblCell.Parse(x.pg.Data[x.ptr..]));

    private static IEnumerable<Page> LeafPages(Page page, Db db) {
        var type = page.Header.PageType;
        if (type == BTreePage.LeafTable)
            return page.Yield();
        if (type != BTreePage.InteriorTable)
            throw new InvalidOperationException($"Cannot get leaf pages of {type}.");
        var rightMostChildPage = page.Header.RightMostPtr
            ?? throw new InvalidOperationException($"Expected {type} to have right most child page pointer.");

        return page
            .CellPtrs()
            .Select(ptr => IntrTblCell.Parse(page.Data[ptr..]))
            .Select(cell => Page.Parse(cell.ChildPage, db))
            .Append(() => Page.Parse(rightMostChildPage, db))
            .SelectMany(p => LeafPages(p, db));
    }
}
