using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;

namespace S3CompareService.UI.Common;

/// <summary>
/// Attached property that makes PasswordBox.Password bindable.
/// The service returns the literal mask "********" for stored secrets; the mask is shown as-is
/// and sent back unchanged unless the user types a replacement. Focusing a box that holds the
/// mask selects it, so typing or pasting replaces it instead of appending to the mask.
/// </summary>
public static class PasswordBoxAssist
{
    public const string Mask = "********";

    public static readonly DependencyProperty BoundPasswordProperty =
        DependencyProperty.RegisterAttached(
            "BoundPassword", typeof(string), typeof(PasswordBoxAssist),
            new FrameworkPropertyMetadata(null, FrameworkPropertyMetadataOptions.BindsTwoWayByDefault, OnBoundPasswordChanged));

    private static readonly DependencyProperty IsUpdatingProperty =
        DependencyProperty.RegisterAttached("IsUpdating", typeof(bool), typeof(PasswordBoxAssist), new PropertyMetadata(false));

    private static readonly DependencyProperty IsAttachedProperty =
        DependencyProperty.RegisterAttached("IsAttached", typeof(bool), typeof(PasswordBoxAssist), new PropertyMetadata(false));

    public static string GetBoundPassword(DependencyObject d) => (string)d.GetValue(BoundPasswordProperty);
    public static void SetBoundPassword(DependencyObject d, string value) => d.SetValue(BoundPasswordProperty, value);

    private static void OnBoundPasswordChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
    {
        if (d is not PasswordBox box) return;

        if (!(bool)box.GetValue(IsAttachedProperty))
        {
            box.SetValue(IsAttachedProperty, true);
            box.PasswordChanged += OnPasswordChanged;
            box.GotKeyboardFocus += OnGotKeyboardFocus;
        }

        if ((bool)box.GetValue(IsUpdatingProperty)) return;

        string newValue = e.NewValue as string ?? string.Empty;
        if (box.Password != newValue)
        {
            box.SetValue(IsUpdatingProperty, true);
            box.Password = newValue;
            box.SetValue(IsUpdatingProperty, false);
        }
    }

    private static void OnPasswordChanged(object sender, RoutedEventArgs e)
    {
        var box = (PasswordBox)sender;
        if ((bool)box.GetValue(IsUpdatingProperty)) return;

        box.SetValue(IsUpdatingProperty, true);
        SetBoundPassword(box, box.Password);
        box.SetValue(IsUpdatingProperty, false);
    }

    private static void OnGotKeyboardFocus(object sender, KeyboardFocusChangedEventArgs e)
    {
        var box = (PasswordBox)sender;
        if (box.Password == Mask) box.SelectAll();
    }
}
