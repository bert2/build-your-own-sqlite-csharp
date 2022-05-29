namespace codecrafters_sqlite;

using static System.Buffers.Binary.BinaryPrimitives;

public static class DbHeader {
    public const byte Size = 100;
    public static ushort PageSize(ReadOnlyMemory<byte> db) => ReadUInt16BigEndian(db.Span[16..18]);
}
