namespace codecrafters_sqlite;

using static System.Buffers.Binary.BinaryPrimitives;

public enum BTreePage {
    IntrIdx = 2,
    IntrTbl = 5,
    LeafIdx = 10,
    LeafTbl = 13,
}

public record PageHeader(
    BTreePage PageType,
    ushort FirstFreeBlockStart,
    ushort NumberOfCells,
    ushort StartOfContentArea,
    byte FragmentedFreeBytes,
    int? RightMostPtr) {
    public const byte MinSize = 8;
    public const byte MaxSize = 12;

    public static PageHeader Parse(ReadOnlyMemory<byte> stream) {
        var bytes = stream.Span;
        var type = bytes[0] switch {
            2 => BTreePage.IntrIdx,
            5 => BTreePage.IntrTbl,
            10 => BTreePage.LeafIdx,
            13 => BTreePage.LeafTbl,
            var x => throw new InvalidOperationException($"Invalid page value encountered: {x}")
        };
        var freeStart = ReadUInt16BigEndian(bytes[1..3]);
        var numCells = ReadUInt16BigEndian(bytes[3..5]);
        var contentStart = ReadUInt16BigEndian(bytes[5..7]);
        var fragFreeBytes = bytes[7];
        int? rightMostPtr = type is BTreePage.IntrTbl or BTreePage.IntrIdx
            ? ReadInt32BigEndian(bytes[8..12])
            : null;

        return new(type, freeStart, numCells, contentStart, fragFreeBytes, rightMostPtr);
    }

    public byte Size() => PageType switch {
        BTreePage.LeafTbl => MinSize,
        BTreePage.LeafIdx => MinSize,
        _ => MaxSize
    };
}
