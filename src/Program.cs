﻿using codecrafters_sqlite;

using static System.StringComparison;

var (path, command) = args.Length switch {
    0 => throw new InvalidOperationException("Missing <database path> and <command>"),
    1 => throw new InvalidOperationException("Missing <command>"),
    _ => (args[0], args[1])
};

var db = Db.FromFile(path);

switch (command) {
    case ".dbinfo": DbInfo(db); break;
    case ".tables": Tables(db); break;
    case ".schema": Schema(db); break;
    case var sql when sql.StartsWith("SELECT COUNT(*) FROM ", OrdinalIgnoreCase):
        Count(db, sql);
        break;
    case var sql: {
        var selectStmt = Sql.ParseSelectStmt(sql);
        var dbSchema = DbSchema.Parse(db);
        var tblSchema = dbSchema.Tbl(selectStmt.Tbl);
        var idxSchema = dbSchema.Idx(selectStmt.Tbl);
        var eval = new Eval(tblSchema, idxSchema);
        var tblPage = Page.Parse(tblSchema.RootPage, db);

        if (selectStmt.Filter?.Col == "id")
            PkScan(db, selectStmt, eval, tblPage);
        else if (selectStmt.Filter != null && eval.HasIdx(selectStmt.Filter.Col))
            IdxScan(db, selectStmt, idxSchema, eval, tblPage);
        else
            TblScan(db, selectStmt, eval, tblPage);

        break;
    }
}

static void DbInfo(Db db) {
    var numTables = DbSchema.Parse(db).Tbls.Count();
    Console.WriteLine($"number of tables: {numTables}");
}

static void Tables(Db db) {
    var names = DbSchema.Parse(db)
        .Tbls
        .Select(tbl => tbl.Name)
        .Join(' ');
    Console.WriteLine(names);
}

static void Schema(Db db) {
    var createStmts = DbSchema.Parse(db)
        .Tbls
        .Select(tbl => tbl.Sql)
        .Join('\n');
    Console.WriteLine(createStmts);
}

static void Count(Db db, string sql) {
    var tblName = sql.Split(' ')[3];
    var tblSchema = DbSchema.Parse(db).Tbl(tblName);
    var count = Page.Parse(tblSchema.RootPage, db).Header.NumberOfCells;
    Console.WriteLine(count);
}

static void PkScan(Db db, SelectStmt selectStmt, Eval eval, Page tblPage) {
    var intPk = selectStmt.Filter?.Val.As<IntValue>()?.Val
        ?? throw new InvalidOperationException($"Filter value {selectStmt.Filter?.Val} is not an integer primary key.");
    var cell = BTree.IntPkScan(intPk, tblPage, db);

    if (cell != null) {
        var row = selectStmt.Cols
            .Select(col => eval.ColValue(col, cell).Render())
            .Join('|');
        Console.WriteLine(row);
    }
}

static void IdxScan(Db db, SelectStmt selectStmt, ObjSchema? idxSchema, Eval eval, Page tblPage) {
    var idxPage = Page.Parse(idxSchema!.RootPage, db);
    var key = selectStmt.Filter?.Val.As<StrValue>()?.Val
        ?? throw new InvalidOperationException($"Filter value {selectStmt.Filter?.Val} cannot be used for index scanning. Indexes are only supported on text columns.");
    var rows = BTree.IdxScan(key, idxPage, tblPage, db)
        .Select(cell => selectStmt.Cols
            .Select(col => eval.ColValue(col, cell).Render())
            .Join('|'))
        .Join('\n');
    Console.WriteLine(rows);
}

static void TblScan(Db db, SelectStmt selectStmt, Eval eval, Page tblPage) {
    var rows = BTree.TblScan(tblPage, db)
        .Where(cell
            => selectStmt.Filter is null
            || eval.ColValue(selectStmt.Filter.Col, cell).Equals(selectStmt.Filter.Val))
        .Select(cell => selectStmt.Cols
            .Select(col => eval.ColValue(col, cell).Render())
            .Join('|'))
        .Join('\n');
    Console.WriteLine(rows);
}
