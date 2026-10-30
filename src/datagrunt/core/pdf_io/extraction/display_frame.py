"""The page display frame: where content renders, in PDF points.

The display frame has its origin at the displayed top-left corner of the
page's visible box (the CropBox intersected with the MediaBox), x growing
rightward and y downward, with /Rotate applied. It is the frame a render of
the page draws in and the one the v4 pdfium engine reports (#384). Table
boxes are mapped into it too (#385), so this module holds the single copy of
the rotation formulas that both use.
"""


def display_edges(left, bottom, right, top, rotation, visible_box):
    """Map unrotated PDF user-space bounds to display-space ``(x0, y_top, x1, y_bot)``.

    PDF user space has a bottom-left origin and ignores /Rotate. The page
    displays its visible box ``(L, B, R, T)`` turned by the page rotation, so
    every edge is measured from that box's displayed top-left corner. Results
    share the frame of the rotation-aware page size and land within it. On an
    uncropped page with a zero origin this reduces to the plain size-only
    formulas. Returns unrounded floats so callers round exactly once.
    """
    vis_left, vis_bottom, vis_right, vis_top = visible_box
    r = rotation % 360
    if r == 90:
        return bottom - vis_bottom, left - vis_left, top - vis_bottom, right - vis_left
    if r == 180:
        return vis_right - right, bottom - vis_bottom, vis_right - left, top - vis_bottom
    if r == 270:
        return vis_top - top, vis_right - right, vis_top - bottom, vis_right - left
    # rotation 0 (and any unexpected value)
    return left - vis_left, vis_top - top, right - vis_left, vis_top - bottom
