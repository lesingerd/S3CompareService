using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;
using S3CompareService.Core.Models;

namespace S3CompareService.UI.Converters;

public class DifferenceTypeToBrushConverter : IValueConverter
{
    private static readonly SolidColorBrush BrushMissingB = new(Color.FromRgb(244, 135, 113));  // Coral / Red
    private static readonly SolidColorBrush BrushMissingA = new(Color.FromRgb(206, 145, 120));  // Light Coral
    private static readonly SolidColorBrush BrushSize = new(Color.FromRgb(220, 220, 170));      // Warm Yellow
    private static readonly SolidColorBrush BrushETag = new(Color.FromRgb(156, 220, 254));      // Soft Cyan
    private static readonly SolidColorBrush BrushContent = new(Color.FromRgb(235, 87, 87));     // High Alert Red
    private static readonly SolidColorBrush BrushGap = new(Color.FromRgb(197, 134, 192));       // Lavender / Purple
    private static readonly SolidColorBrush BrushDefault = new(Color.FromRgb(204, 204, 204));

    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is DifferenceType diffType)
        {
            return diffType switch
            {
                DifferenceType.MissingInBucketB => BrushMissingB,
                DifferenceType.MissingInBucketA => BrushMissingA,
                DifferenceType.SizeMismatch => BrushSize,
                DifferenceType.ETagMismatch => BrushETag,
                DifferenceType.ContentMismatch => BrushContent,
                DifferenceType.PossibleMissingOnBothSides => BrushGap,
                _ => BrushDefault
            };
        }
        return BrushDefault;
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) => throw freshNotSupported();
    private static NotSupportedException freshNotSupported() => new();
}

public class GracePeriodTagConverter : IValueConverter
{
    private static readonly SolidColorBrush InFlightBrush = new(Color.FromRgb(220, 180, 80)); // Amber / Warning
    private static readonly SolidColorBrush ConfirmedBrush = new(Color.FromRgb(244, 100, 100)); // Confirmed Disparity Red

    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        bool isWithin = value is bool b && b;
        if (parameter?.ToString() == "Text")
        {
            return isWithin ? "IN-FLIGHT REPLICATION" : "CONFIRMED DISPARITY";
        }
        return isWithin ? InFlightBrush : ConfirmedBrush;
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) => throw new NotSupportedException();
}

public class InverseBooleanConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is bool b) return !b;
        return true;
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is bool b) return !b;
        return false;
    }
}

public class IntToBooleanConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is int val && parameter != null && int.TryParse(parameter.ToString(), out int target))
        {
            return val == target;
        }
        return false;
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is bool b && b && parameter != null && int.TryParse(parameter.ToString(), out int target))
        {
            return target;
        }
        return System.Windows.Data.Binding.DoNothing;
    }
}

public class ServiceStatusToBrushConverter : IValueConverter
{
    private static readonly SolidColorBrush RunningBrush = new(Color.FromRgb(129, 199, 132)); // Green #81C784
    private static readonly SolidColorBrush StoppedBrush = new(Color.FromRgb(244, 135, 113)); // Coral Red #F48771

    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        string status = value?.ToString() ?? string.Empty;
        return status.Equals("Running", StringComparison.OrdinalIgnoreCase) ? RunningBrush : StoppedBrush;
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) => throw new NotSupportedException();
}

public class EnabledStateConverter : IValueConverter
{
    private static readonly SolidColorBrush EnabledBgBrush = new(Color.FromRgb(46, 125, 50));   // #2E7D32
    private static readonly SolidColorBrush DisabledBgBrush = new(Color.FromRgb(62, 62, 66));  // #3E3E42

    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        bool isEnabled = value is bool b && b;
        if (parameter?.ToString() == "Text")
        {
            return isEnabled ? "ENABLED" : "DISABLED";
        }
        return isEnabled ? EnabledBgBrush : DisabledBgBrush;
    }

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) => throw new NotSupportedException();
}

