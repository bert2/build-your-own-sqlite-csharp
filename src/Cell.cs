namespace codecrafters_sqlite;

using System;

using static System.Buffers.Binary.BinaryPrimitives;

public record LeafTblCell(long RowId, Record Payload) {
    public static LeafTblCell Parse(ReadOnlyMemory<byte> data) {
        var (payloadSize, bytesRead1) = Varint.Parse(data);
        var (rowId, bytesRead2) = Varint.Parse(data[bytesRead1..]);
        var payloadStart = bytesRead1 + bytesRead2;
        var payload = Record.Parse(data.Slice(payloadStart, checked((int)payloadSize)));
        return new(rowId, payload);
    }
}

public record IntrTblCell(long RowId, int ChildPage) {
    public static IntrTblCell Parse(ReadOnlyMemory<byte> data) {
        var childPage = ReadInt32BigEndian(data[..4].Span);
        var (rowId, _) = Varint.Parse(data[4..]);
        return new(checked((int)rowId), childPage);
    }
}
