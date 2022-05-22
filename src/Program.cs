using codecrafters_sqlite;
using MoreLinq;

using static System.StringComparison;

var (path, command) = args.Length switch {
    0 => throw new InvalidOperationException("Missing <database path> and <command>"),
    1 => throw new InvalidOperationException("Missing <command>"),
    _ => (args[0], args[1])
};

var db = Db.FromFile(path);

switch (command) {
    case ".dbinfo": {
        var numTables = DbSchema.Parse(db).Tbls.Count();

        Console.WriteLine($"number of tables: {numTables}");
        break;
    }
    case ".tables": {
        var names = DbSchema.Parse(db)
            .Tbls
            .Select(schema => schema.Name)
            .Join(' ');

        Console.WriteLine(names);
        break;
    }
    case ".schema": {
        var createStmts = DbSchema.Parse(db)
            .Tbls
            .Select(schema => schema.Sql)
            .Join('\n');

        Console.WriteLine(createStmts);
        break;
    }
    case var sql when sql.StartsWith("SELECT COUNT(*) FROM ", OrdinalIgnoreCase): {
        var tblName = sql.Split(' ')[3];
        var tblSchema = DbSchema.Parse(db).Tbl(tblName);
        var count = Page.Parse(tblSchema.RootPage, db).Header.NumberOfCells;

        Console.WriteLine(count);
        break;
    }
    case var sql: {
        var selectStmt = Sql.ParseSelectStmt(sql);
        var tblSchema = DbSchema.Parse(db).Tbl(selectStmt.Tbl);
        var colIdxs = Sql.ParseCreateTblStmt(tblSchema.Sql)
            .Cols
            .Index()
            .ToDictionary(x => x.Value, x => x.Key);

        var page = Page.Parse(tblSchema.RootPage, db);
        var rows = BTree
            .FullTblScan(page, db)
            .Where(cell
                => selectStmt.Filter is null
                || cell.Payload[colIdxs[selectStmt.Filter.Col]].ToUtf8String() == selectStmt.Filter.Val)
            .Select(cell => selectStmt.Cols
                .Select(col => col == "id" ? cell.RowId.ToString() : cell.Payload[colIdxs[col]].Render())
                .Join('|'))
            .Join('\n');

        Console.WriteLine(rows);
        break;
    }
}
