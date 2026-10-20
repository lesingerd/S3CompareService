namespace S3CompareService.Core.Engine;

/// <summary>
/// Orders strings by Unicode code point, which is identical to the order of their UTF-8 byte sequences.
/// S3 lists keys in UTF-8 binary order, whereas <see cref="string.CompareOrdinal(string, string)"/> compares
/// UTF-16 code units, which differs for supplementary characters (surrogates, e.g. emoji) versus
/// U+E000..U+FFFF (e.g. fullwidth forms U+FF01): in UTF-8 U+FF01 sorts before U+1F600, in UTF-16 it sorts after.
/// </summary>
public sealed class Utf8OrdinalComparer : IComparer<string>, IEqualityComparer<string>
{
    public static readonly Utf8OrdinalComparer Instance = new();

    public int Compare(string? x, string? y) => CompareStatic(x, y);

    public bool Equals(string? x, string? y) => string.Equals(x, y, StringComparison.Ordinal);

    public int GetHashCode(string obj) => obj.GetHashCode();

    public static int CompareStatic(string? x, string? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;

        int len = Math.Min(x.Length, y.Length);
        for (int i = 0; i < len; i++)
        {
            char a = x[i];
            char b = y[i];
            if (a == b) continue;

            // Both in the surrogate / high-BMP range: surrogates (supplementary planes) must sort after U+E000..U+FFFF.
            if (a >= 0xD800 && b >= 0xD800)
            {
                a = Fixup(a);
                b = Fixup(b);
            }
            return a - b;
        }

        return x.Length - y.Length;
    }

    private static char Fixup(char c) => c >= 0xE000 ? (char)(c - 0x800) : (char)(c + 0x2000);
}
