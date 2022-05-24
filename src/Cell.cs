namespace codecrafters_sqlite;

using System;

using static System.Buffers.Binary.BinaryPrimitives;

public record LeafTblCell(long RowId, Record Payload) {
    public static LeafTblCell Parse(ReadOnlyMemory<byte> data) {
        var (payloadSize, bytesRead1) = Varint32.Parse(data);
        var (rowId, bytesRead2) = Varint64.Parse(data[bytesRead1..]);
        var payloadStart = bytesRead1 + bytesRead2;
        var payload = Record.Parse(data.Slice(payloadStart, payloadSize));
        return new(rowId, payload);
    }
}

public record IntrTblCell(long RowId, int ChildPage) {
    public static IntrTblCell Parse(ReadOnlyMemory<byte> data) {
        var childPage = ReadInt32BigEndian(data[..4].Span);
        var (rowId, _) = Varint64.Parse(data[4..]);
        return new(rowId, childPage);
    }
}

public record LeafIdxCell(Record Payload) {
    public static LeafIdxCell Parse(ReadOnlyMemory<byte> data) {
        var (payloadSize, bytesRead) = Varint32.Parse(data);
        var payload = Record.Parse(data.Slice(bytesRead, payloadSize));
        return new(payload);
    }
}

public record IntrIdxCell(int ChildPage, Record Payload) {
    public static IntrIdxCell Parse(ReadOnlyMemory<byte> data) {
        var childPage = ReadInt32BigEndian(data[..4].Span);
        var (payloadSize, bytesRead) = Varint32.Parse(data[4..]);
        var payloadStart = 4 + bytesRead;
        var payload = Record.Parse(data.Slice(payloadStart, payloadSize));
        return new(childPage, payload);
    }
}
