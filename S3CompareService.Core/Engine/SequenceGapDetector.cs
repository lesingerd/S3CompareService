using System.Text.RegularExpressions;
using S3CompareService.Core.Models;

namespace S3CompareService.Core.Engine;

public class SequenceGapDetector
{
    private static readonly Regex NumericGroupRegex = new(@"\d+", RegexOptions.Compiled);

    private readonly int _maxGapThreshold;
    private string? _currentSeriesSignature;
    private long _currentSeriesLastNumber = -1;
    private int _currentSeriesPadding = 0;
    private string _currentSeriesLastKey = string.Empty;

    public SequenceGapDetector(int maxGapThreshold = 25)
    {
        _maxGapThreshold = maxGapThreshold > 0 ? maxGapThreshold : 25;
    }

    public List<RecordedDifference> CheckKey(string key, string scanRunId, ref long differenceIdCounter)
    {
        var gaps = new List<RecordedDifference>();
        if (string.IsNullOrWhiteSpace(key)) return gaps;

        int lastSlash = key.LastIndexOf('/');
        string dir = lastSlash >= 0 ? key.Substring(0, lastSlash + 1) : "";
        string fileName = lastSlash >= 0 ? key.Substring(lastSlash + 1) : key;

        var matches = NumericGroupRegex.Matches(fileName);
        if (matches.Count == 0)
        {
            _currentSeriesSignature = null;
            return gaps;
        }

        // Use the last numeric group in the filename
        var lastMatch = matches[^1];
        if (!long.TryParse(lastMatch.Value, out long currNumber))
        {
            _currentSeriesSignature = null;
            return gaps;
        }

        string prefix = fileName.Substring(0, lastMatch.Index);
        string digits = lastMatch.Value;
        string suffix = fileName.Substring(lastMatch.Index + lastMatch.Length);
        string seriesSignature = dir + prefix + "|" + suffix;

        // Gaps are only meaningful when every numeric group in the series has the same width, i.e. when
        // lexicographic (S3 listing) order equals numeric order. Unpadded names (file1, file10, file2, ...)
        // change width mid-series, so a width change restarts the series instead of reporting phantom gaps.
        if (seriesSignature == _currentSeriesSignature && digits.Length == _currentSeriesPadding)
        {
            if (currNumber > _currentSeriesLastNumber + 1)
            {
                long gapCount = currNumber - (_currentSeriesLastNumber + 1);
                if (gapCount > 0 && gapCount <= _maxGapThreshold)
                {
                    int padLength = digits.Length;
                    for (long missing = _currentSeriesLastNumber + 1; missing < currNumber; missing++)
                    {
                        string missingStr = missing.ToString().PadLeft(padLength, '0');
                        string missingKey = dir + prefix + missingStr + suffix;

                        gaps.Add(new RecordedDifference
                        {
                            Id = ++differenceIdCounter,
                            ScanRunId = scanRunId,
                            Key = missingKey,
                            Type = DifferenceType.PossibleMissingOnBothSides,
                            Details = $"Synthetic sequence gap: File #{missing} appears missing from series '{prefix}*{suffix}' on both sides. Preceding: '{_currentSeriesLastKey}', Following: '{key}'",
                            DetectedAtUtc = DateTime.UtcNow,
                            IsWithinGracePeriod = false // Sequence gaps represent permanent numbering skips
                        });
                    }
                }
            }
            _currentSeriesLastNumber = currNumber;
            _currentSeriesLastKey = key;
            _currentSeriesPadding = digits.Length;
        }
        else
        {
            _currentSeriesSignature = seriesSignature;
            _currentSeriesLastNumber = currNumber;
            _currentSeriesLastKey = key;
            _currentSeriesPadding = digits.Length;
        }

        return gaps;
    }

    public void Reset()
    {
        _currentSeriesSignature = null;
        _currentSeriesLastNumber = -1;
        _currentSeriesPadding = 0;
        _currentSeriesLastKey = string.Empty;
    }
}
