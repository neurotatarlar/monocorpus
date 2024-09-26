import json
import os.path

import pymupdf
from huggingface_hub import hf_hub_download
from rich.progress import track
from ultralytics import YOLO

from consts import Dirs
from file_utils import get_path_in_workdir
from integration.s3 import upload_files_to_s3

REPO_ID = 'hantian/yolo-doclaynet'
MODEL_NAME = 'yolov10b'
MODEL_CHECKPOINT = f"{MODEL_NAME}-doclaynet.pt"
DPI = 100


def layout_analysis(path_to_file: str, md5: str, pages_slice):
    """
    Analyze the page_layout of the document

    First, the function makes images of the book pages_slice.
    Then, it analyzes the page_layout of the pages_slice and uploads the images and the page_layout analysis to S3.
    """
    # Make images of the book pages_slice
    pages_details, pages_count = _make_images_of_pages(path_to_file, md5, pages_slice)
    # Analyze the page_layout of the pages_slice
    pages_details = _create_layout_analysis(pages_details, md5, path_to_file)
    # Upload the images to S3
    remote_files = upload_files_to_s3(
        [f['path_to_image'] for f in pages_details.values()],
        lambda c: c['yc']['bucket']['images'],
        Prefix=md5
    )
    # Update the paths to the remote images
    for page_no, details in pages_details.items():
        details["remote_path"] = remote_files[details["path_to_image"]]

    # Export the predictions to the Label Studio format
    tasks_paths = _export_predictions(pages_details, md5)
    # The first task is the title task, upload it separately
    title_tasks = []
    for i in range(0, 3):
        if task := tasks_paths.pop(i, None):
            title_tasks.append(task)
    if title_tasks:
        upload_files_to_s3(
            title_tasks,
            lambda c: c['yc']['bucket']['title_tasks'],
            Prefix=md5
        )
    if tasks_paths:
        # Upload the rest of the tasks
        upload_files_to_s3(
            tasks_paths.values(),
            lambda c: c['yc']['bucket']['tasks'],
            Prefix=md5
        )
    return pages_count


def _make_images_of_pages(path_to_file: str, md5: str, pages=None):
    output_dir = os.path.join(get_path_in_workdir(Dirs.PAGE_IMAGES), md5)
    os.makedirs(output_dir, exist_ok=True)
    results = {}
    with pymupdf.open(path_to_file) as doc:
        pages_count = doc.page_count
        for page in track(doc[pages], description=f"Extracting images of the pages_slice from `{md5}`..."):
            path_to_image = os.path.join(output_dir, f"{page.number}.png")
            if os.path.exists(path_to_image):
                pix = pymupdf.Pixmap(path_to_image)
            else:
                pix = page.get_pixmap(colorspace='rgb', alpha=False, dpi=DPI)
                pix.save(path_to_image, 'png')
            results[page.number] = {
                "path_to_image": path_to_image,
                "width": pix.width,
                "height": pix.height
            }
    return results, pages_count


def _create_layout_analysis(pages_details, md5: str, path_to_file: str):
    pages_details = _inference(pages_details, md5)
    pages_details = _post_process_layouts(pages_details, path_to_file)
    return pages_details


def _inference(pages_details, md5: str):
    # Create a directory for the plots
    plots_dir = os.path.join(get_path_in_workdir(Dirs.BOXES_PLOTS), md5)
    os.makedirs(plots_dir, exist_ok=True)

    model = YOLO(hf_hub_download(repo_id=REPO_ID, filename=MODEL_CHECKPOINT))

    for page_no, details in track(pages_details.items(), description=f"Analyzing layouts of the `{md5}`..."):
        pred = model.predict(details["path_to_image"], verbose=False, imgsz=1024)
        results = pred[0].cpu()
        boxes = results.boxes.xyxy.numpy()
        confs = results.boxes.conf.numpy()
        classes = results.boxes.cls.numpy()

        page_layouts = [
            {
                "bbox": x[0].tolist(),
                "class": results.names[int(x[2])].lower(),
                "conf": str(round(x[1], 2)),
                "id": f"{page_no}::{idx}"
            }
            for (idx, x)
            in enumerate(zip(boxes, confs, classes))
        ]
        details["layouts"] = page_layouts

        # Save the plot
        results.save(os.path.join(plots_dir, f"{page_no}.png"))

    return pages_details


def _export_predictions(pages_details, md5: str):
    tasks = {
        page_no: {
            "data": {
                "image": details["remote_path"],
                "page_no": page_no,
                "hash": md5,
            },
            "predictions": [
                {
                    "model_version": "yolo",
                    "result": [
                        {
                            "id": f"{md5}::{layout["id"]}",
                            "type": "rectanglelabels",
                            "from_name": "label",
                            "original_width": details["width"],
                            "original_height": details["height"],
                            "to_name": "image",
                            "image_rotation": 0,
                            "readonly": False,
                            "value": {
                                "rotation": 0,
                                "x": layout["bbox"][0] * 100 / details["width"],
                                "y": layout["bbox"][1] * 100 / details["height"],
                                "width": (layout["bbox"][2] - layout["bbox"][0]) * 100 / details["width"],
                                "height": (layout["bbox"][3] - layout["bbox"][1]) * 100 / details["height"],
                                "rectanglelabels": [
                                    layout["class"]
                                ],
                            }
                        }
                        for layout in sorted(details["layouts"], key=lambda x: (x["bbox"][1], x["bbox"][0]))
                    ],
                }
            ]
        }
        for page_no, details
        in pages_details.items()
    }
    tasks_dir = os.path.join(get_path_in_workdir(Dirs.LABEL_STUDIO_TASKS), md5)
    os.makedirs(tasks_dir, exist_ok=True)
    tasks_paths = {}
    for page_no, task in tasks.items():
        output_path = os.path.join(tasks_dir, f"{page_no}.json")
        with open(output_path, "w") as f:
            json.dump(task, f, indent=4)
        tasks_paths[page_no] = output_path

    return tasks_paths


def _post_process_layouts(pages_details, path_to_file):
    for page_no, details in pages_details.items():
        details["layouts"] = _iou(details)
        _semantic_transform(details, page_no, path_to_file)
    return pages_details


def _iou(page_layout, threshold=0.8):
    """
    Post-process the page_layout analysis results.
    It aimed to mitigate the problem of overlapping bounding boxes.

    threshold: float (default=0.8) - the threshold for the intersection over union (IoU) to consider two bounding boxes
    as the same region.
    """

    def intersection_area(box1, box2):
        """Calculate the intersection area of two bounding boxes."""
        x1 = max(box1[0], box2[0])
        y1 = max(box1[1], box2[1])
        x2 = min(box1[2], box2[2])
        y2 = min(box1[3], box2[3])
        return calculate_area((x1, y1, x2, y2))

    def calculate_area(box):
        """Calculate the area of a bounding box."""
        x1, y1, x2, y2 = box
        return max(0, x2 - x1) * max(0, y2 - y1)

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

    layouts = list(map(lambda x: (x, calculate_area(x['bbox'])), page_layout['layouts']))
    changed = True
    while changed:
        changed = False
        new_layouts = []

        while layouts:
            this_region, this_area = layouts.pop(0)
            merged = False

            for idx, (other_region, other_area) in enumerate(layouts):
                inter_area = intersection_area(this_region['bbox'], other_region['bbox'])
                ratio_inter_to_box1 = inter_area / this_area
                ratio_inter_to_box2 = inter_area / other_area

                if ratio_inter_to_box1 > threshold and ratio_inter_to_box2 > threshold:
                    # merge the two regions, because their intersection area is big to consider them as one region
                    merged_region = merge_regions((this_region, this_area), (other_region, other_area))
                    merged_area = calculate_area(merged_region['bbox'])
                    new_layouts.append((merged_region, merged_area))
                    layouts.pop(idx)
                    changed = True
                    merged = True
                    break
                elif ratio_inter_to_box2 > threshold or ratio_inter_to_box1 > threshold:
                    # one region is almost full inside the other region, so discard one of them
                    if pick_first((this_region, this_area), (other_region, other_area)):
                        _ = layouts.pop(idx)
                    else:
                        merged = True
                    changed = True
                    break

            if not merged:
                new_layouts.append((this_region, this_area))

        layouts = new_layouts[:]

    return [x[0] for x in layouts]


def _semantic_transform(page_layout, page_no, path_to_file):
    """
    Post-process the page_layout analysis results based on knowledge how books typically look like.
    """
    sorted_layout = sorted(page_layout["layouts"], key=lambda x: (x["bbox"][1], x["bbox"][0]))

    def middle(b):
        return (b[1] + b[3]) / 2

    def calculate_bbox(_page, _bbox):
        width_ratio = _page.rect.width / page_layout['width']
        height_ratio = _page.rect.height / page_layout['height']
        x1 = _bbox[0] * width_ratio
        y1 = _bbox[1] * height_ratio
        x2 = _bbox[2] * width_ratio
        y2 = _bbox[3] * height_ratio
        return x1, y1, x2, y2

    def first_span_is_superscript(p, b):
        prev_span = None
        for block in p.get_text("dict", clip=b, sort=True).get("blocks", []):
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    text = span.get("text")
                    text = text.strip() if text else None
                    if text and (text.isnumeric() or text.startswith('*')) and not prev_span:
                        # here if first span is numeric, but we don't know if it is a superscript until we find the
                        # next span
                        prev_span = span
                    elif prev_span:
                        # if we found the next span, then we can compare the font size of the previous span and the
                        # current span, if the current span has a bigger font size, then previous span is a superscript
                        prev_span_font_size = prev_span["size"]
                        cur_span_font_size = span["size"]
                        if round(cur_span_font_size, 1) > round(prev_span_font_size, 1):
                            # return True to signal that the first span is a superscript
                            return True
                        else:
                            # return False to signal that the first span is not a superscript
                            return False
                    else:
                        # here if the first span was not numeric, then we can break the loop
                        return False

    def safely_get_span_text(p, b, index):
        try:
            return p.get_text("dict", clip=b, sort=True)["blocks"][index]["lines"][index]["spans"][index]["text"]
        except (IndexError, KeyError):
            return None

    with pymupdf.open(path_to_file) as doc:
        for idx, l in enumerate(sorted_layout):
            # title class can be found once on the first page, so if the page number is greater than 0, then replace all
            # `titles` with `section-header
            this_class = l["class"]
            if page_no > 0 and this_class == "title":
                this_class = "section-header"
            # replace all `list-item` with `text` because there are a lot of errors on this class
            # if this is a `text` and it is the first one, and it is in the first 1/6 of the page, then check if it
            # starts or ends with number less than 1000
            elif this_class in ['text', 'section-header'] and idx == 0 and middle(l["bbox"]) < page_layout[
                'height'] / 6:
                page = doc[page_no]
                bbox = calculate_bbox(page, l["bbox"])
                if first_span_text := safely_get_span_text(page, bbox, 0):
                    if first_span_text.isnumeric() and int(first_span_text) < 1000:
                        this_class = 'page-header'
                elif last_span_text := safely_get_span_text(page, bbox, -1):
                    if last_span_text.isnumeric() and int(last_span_text) < 1000:
                        this_class = 'page-header'
            # if this is a `page-footer`, and it is not in the last 1/3 of the page, then replace it with `text`
            elif this_class == 'page-footer' and middle(l["bbox"]) < page_layout['height'] * 2 / 3:
                this_class = 'text'
            # if this is a `page-footer`, and is is non-numeric, then replace it with `text`
            elif this_class == 'page-footer':
                page = doc[page_no]
                bbox = calculate_bbox(page, l["bbox"])
                if first_span_text := safely_get_span_text(page, bbox, 0):
                    if not (first_span_text.isnumeric() and int(first_span_text) < 1000):
                        this_class = 'text'

            l["class"] = this_class

        # Try to detect labeled_footnotes. For this we read all layout elements from the bottom to the top and if the first
        # span is a superscript and number, then we consider it as a footnote
        attempts = 1
        for l in reversed(sorted_layout):
            match l["class"]:
                case "footnote":
                    continue
                case "text" | "page-footer":
                    page = doc[page_no]
                    bbox = calculate_bbox(page, l["bbox"])
                    if first_span_is_superscript(page, bbox):
                        l["class"] = "footnote"
                    elif attempts == 0:
                        break
                    else:
                        attempts -= 1
                case _:
                    break
