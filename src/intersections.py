def compute_area(bbox):
    x1, y1, x2, y2 = bbox
    return max(0, x2 - x1) * max(0, y2 - y1)

def compute_intersection(bbox1, bbox2):
    x1 = max(bbox1[0], bbox2[0])
    y1 = max(bbox1[1], bbox2[1])
    x2 = min(bbox1[2], bbox2[2])
    y2 = min(bbox1[3], bbox2[3])
    return x1, y1, x2, y2

def compute_intersection_area(bbox1, bbox2):
    x1, y1, x2, y2 = compute_intersection(bbox1, bbox2)
    return compute_area((x1, y1, x2, y2))

def pick_first(first, second):
    _l1, _area1 = first
    _l2, _area2 = second

    # choose the one with higher confidence
    if _l1['conf'] > _l2['conf']:
        return True
    elif _l1['conf'] < _l2['conf']:
        return False
    # below if confidences are equal
    # then choose the one with the smaller area
    elif _area1 < _area2:
        return True
    elif _area1 > _area2:
        return False
    # below if confidence and areas are equal (but still can be various classes)
    # then choose the first one
    else:
        return True

def merge_regions(a, b):
    """Merge two bounding boxes."""
    chosen_region, _ = a if pick_first(a, b) else b
    a_bbox, b_bbox = a[0]['bbox'], b[0]['bbox']
    chosen_region['bbox'] = (
        min(a_bbox[0], b_bbox[0]),
        min(a_bbox[1], b_bbox[1]),
        max(a_bbox[2], b_bbox[2]),
        max(a_bbox[3], b_bbox[3])
    )
    return chosen_region

def add_vertical_gap(bbox1, bbox2, gap):
    if bbox1[1] < bbox2[1]:  # bbox1 is above bbox2
        adjustment = max(gap, gap + (bbox1[3] - bbox2[1])) / 2
        print(f"adjustment: {adjustment}, overlap: {bbox1[3] - bbox2[1]}")
        bbox1 = (bbox1[0], bbox1[1], bbox1[2], bbox2[1] - adjustment)
        bbox2 = (bbox2[0], bbox2[1] + adjustment, bbox2[2], bbox2[3])
    else:  # bbox2 is above bbox1
        adjustment = max(gap, gap + (bbox2[3] - bbox1[1])) / 2
        print(f"adjustment: {adjustment}, overlap: {bbox2[3] - bbox1[1]}")
        bbox2 = (bbox2[0], bbox2[1], bbox2[2], bbox1[1] - adjustment)
        bbox1 = (bbox1[0], bbox1[1] + adjustment, bbox1[2], bbox1[3])
    return bbox1, bbox2

def _process_intersections(page_layout, threshold=0.8, gap=2):
    """
    Post-process the page_layout analysis results.
    It aimed to mitigate the problem of overlapping bounding boxes.

    threshold: float (default=0.8) - the threshold for the intersection over union (IoU) to consider two bounding boxes
    as the same region.
    """
    layouts = [x for x in page_layout['layouts']]
    changed = True
    while changed:
        layouts, changed = _process(layouts, threshold, gap)
        layouts = [l for l in layouts if l is not None]
    return layouts

def _process(layouts, threshold, gap):
    for i in range(len(layouts)):
        this_region = layouts[i]

        for j in range(i + 1, len(layouts)):
            other_region = layouts[j]

            this_region_bbox = this_region['bbox']
            other_region_bbox = other_region['bbox']
            intersection_area = compute_intersection_area(this_region_bbox, other_region_bbox)
            area1 = compute_area(this_region_bbox)
            area2 = compute_area(other_region_bbox)

            if not area1 or not area2:
                continue
            elif area1 and area2 and intersection_area / area1 > threshold and intersection_area / area2 > threshold:
                # Strategy 1: Merge
                layouts[i] = merge_regions((this_region, area1), (other_region, area2))
                layouts[j] = None
                return layouts, True
            elif intersection_area / area1 > threshold or intersection_area / area2 > threshold:
                # Strategy 2: Keep one region, discard other
                chosen_region = this_region if pick_first((this_region, area1), (other_region, area2)) else other_region
                layouts[i] = chosen_region
                layouts[j] = None
                return layouts, True
            elif intersection_area > 0:
                # Strategy 3: Add vertical gap
                if this_region_bbox[1] <= other_region_bbox[3] and this_region_bbox[3] >= other_region_bbox[1]:  # Check vertical overlap
                    this_region_bbox, other_region_bbox = add_vertical_gap(this_region_bbox, other_region_bbox, gap)
                    this_region['bbox'] = this_region_bbox
                    other_region['bbox'] = other_region_bbox
                    layouts[i] = this_region
                    layouts[j] = other_region
                    return layouts, True
    return layouts, False




