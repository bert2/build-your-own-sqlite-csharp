namespace codecrafters_sqlite;

using System;

public interface IValue : IEquatable<IValue> {
    string Render();
}

public record NullValue : IValue {
    public string Render() => "NULL";
    public bool Equals(IValue? other) => other is NullValue;
}

public record IntValue(long Val) : IValue {
    public string Render() => Val.ToString();
    public bool Equals(IValue? other) => other is IntValue i && Equals(i);
}

public record StrValue(string Val) : IValue {
    public string Render() => Val;
    public bool Equals(IValue? other) => other is StrValue s && Equals(s);
}

public static class Eval {
    public static IValue ColValue(string col, LeafTblCell cell, Dictionary<string, int> colIdxs)
        => col == "id" ? new IntValue(cell.RowId) : cell.Payload[colIdxs[col]].ToValue();
}
