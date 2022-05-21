namespace codecrafters_sqlite;

using static System.Buffers.Binary.BinaryPrimitives;

public enum BTreePage {
    InteriorIndex = 2,
    InteriorTable = 5,
    LeafIndex = 10,
    LeafTable = 13,
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
            2 => BTreePage.InteriorIndex,
            5 => BTreePage.InteriorTable,
            10 => BTreePage.LeafIndex,
            13 => BTreePage.LeafTable,
            var x => throw new InvalidOperationException($"Invalid page value encountered: {x}")
        };
        var freeStart = ReadUInt16BigEndian(bytes[1..3]);
        var numCells = ReadUInt16BigEndian(bytes[3..5]);
        var contentStart = ReadUInt16BigEndian(bytes[5..7]);
        var fragFreeBytes = bytes[7];
        var rightMostPtr = ReadInt32BigEndian(bytes[8..12]);

        return new(type, freeStart, numCells, contentStart, fragFreeBytes, rightMostPtr);
    }

    public byte Size() => PageType switch {
        BTreePage.LeafTable => MinSize,
        BTreePage.LeafIndex => MinSize,
        _ => MaxSize
    };
}
