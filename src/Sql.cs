#pragma warning disable IDE0065 // Misplaced using directive

using FParsec;
using FParsec.CSharp;
using Microsoft.FSharp.Core;
using static FParsec.CSharp.PrimitivesCS;
using static FParsec.CSharp.CharParsersCS;

namespace codecrafters_sqlite;

using Chars = CharStream<Unit>;
using StringParser = FSharpFunc<CharStream<Unit>, Reply<string>>;

public record Filter(string Col, string Val);

public record SelectStmt(string[] Cols, string Tbl, Filter? Filter);

public record CreateTblStmt(string Tbl, string[] Cols);

public static class Sql
{
    public static SelectStmt ParseSelectStmt(string sql) => selectStmt.Run(sql).GetResult();

    public static CreateTblStmt ParseCreateTblStmt(string sql) => createTblStmt.Run(sql).GetResult();

    private static readonly StringParser stringLiteral =
        Between('\'', ManyChars(c => c != '\''), '\'')
        .Lbl_("string literal");

    private static readonly StringParser regularIdentifier = Many1Chars(
        pred1: c => c == '_' || char.IsLetter(c),
        pred: c => c == '_' || char.IsLetterOrDigit(c),
        label: "regular identifier");

    private static readonly StringParser delimitedIdentifier =
        Between('"', Many1Chars(c => c != '"'), '"')
        .Lbl_("delimited identifier");

    private static readonly StringParser identifier = delimitedIdentifier.Or(regularIdentifier);

    private static readonly StringParser colDef =
        identifier.And(SkipMany(NoneOf(",)"))).Lbl_("column defintion");

    private static readonly FSharpFunc<Chars, Reply<Filter>> whereFilter =
        SkipCI("WHERE").And_(WS1)
        .AndR(identifier).And(WS)
        .And(Skip('=')).And(WS)
        .And(stringLiteral)
        .Map((col, val) => new Filter(col, val));

    private static readonly FSharpFunc<Chars, Reply<SelectStmt>> selectStmt =
        SkipCI("SELECT").And_(WS1)
        .AndR(Many1(identifier.And(WS), sep: CharP(',').And(WS)))
        .And(SkipCI("FROM")).And(WS1)
        .And(identifier).And(WS)
        .And(Opt(whereFilter)).And(WS)
        .And(EOF)
        .Map(Flat)
        .Map((cols, tbl, filt) => new SelectStmt(cols.ToArray(), tbl, filt))
        .Lbl_("SELECT statement");

    private static readonly FSharpFunc<Chars, Reply<CreateTblStmt>> createTblStmt =
        SkipCI("CREATE").And_(WS1)
        .And_(SkipCI("TABLE")).And_(WS1)
        .AndR(identifier).And(WS)
        .And(Between(
            open: CharP('(').And(WS),
            Many1(colDef, sep: CharP(',').And(WS)),
            close: WS.And(CharP(')')).And(WS)))
        .And(EOF)
        .Map((tbl, cols) => new CreateTblStmt(tbl, cols.ToArray()))
        .Lbl_("CREATE TABLE statement");
}
