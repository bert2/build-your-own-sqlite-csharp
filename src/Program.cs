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
        var numTables = Schema.ParseAll(db).Count();

        Console.WriteLine($"number of tables: {numTables}");
        break;
    }
    case ".tables": {
        var names = Schema.ParseAll(db)
            .Select(schema => schema.Name)
            .Join(" ");

        Console.WriteLine(names);
        break;
    }
    case var sql when sql.StartsWith("SELECT COUNT(*) FROM ", OrdinalIgnoreCase): {
        var tblName = sql.Split(' ')[3];
        var tblSchema = Schema.ParseAll(db).First(schema => schema.Name == tblName);

        var count = Page.Parse(tblSchema.RootPage, db).Header.NumberOfCells;

        Console.WriteLine(count);
        break;
    }
    case var sql: {
        var selectStmt = Sql.ParseSelectStmt(sql);
        var tblSchema = Schema.ParseAll(db)
            .First(schema => schema.Name == selectStmt.Tbl);
        var colIdxs = Sql.ParseCreateTblStmt(tblSchema.Sql)
            .Cols
            .Index()
            .ToDictionary(x => x.Value, x => x.Key);

        var page = Page.Parse(tblSchema.RootPage, db);
        var rows = BTree
            .FullTblScan(page, db)
            .Select(cell => cell.Payload)
            .Where(record
                => selectStmt.Filter is null
                || record[colIdxs[selectStmt.Filter.Col]].ToUtf8String() == selectStmt.Filter.Val)
            .Select(record => selectStmt
                .Cols
                .Select(col => record[colIdxs[col]].Render())
                .Join("|"))
            .Join("\n");

        Console.WriteLine(rows);
        break;
    }
}
