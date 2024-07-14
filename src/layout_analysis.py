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
DPI = 300


def layout_analysis(path_to_file: str, md5: str, pages_slice):
    """
    Analyze the layout of the document

    First, the function makes images of the book pages_slice.
    Then, it analyzes the layout of the pages_slice and uploads the images and the layout analysis to S3.
    """
    # Make images of the book pages_slice
    pages_details, pages_count = _make_images_of_pages(path_to_file, md5, pages_slice)
    # Analyze the layout of the pages_slice
    pages_details = _create_layout_analysis(pages_details, md5)
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
    # Upload the task to S3
    upload_files_to_s3(
        tasks_paths,
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


def _create_layout_analysis(pages_details, md5: str):
    # Create a directory for the plots
    plots_dir = os.path.join(get_path_in_workdir(Dirs.BOXES_PLOTS), md5)
    os.makedirs(plots_dir, exist_ok=True)

    model = YOLO(hf_hub_download(repo_id=REPO_ID, filename=MODEL_CHECKPOINT))

    for page_no, details in track(pages_details.items(), description=f"Analyzing layouts of the pages_slice from `{md5}`..."):
        pred = model.predict(details["path_to_image"], verbose=False)
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
        details["layouts"] = _post_process_layouts(page_layouts, page_no)

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
                    "model_version": MODEL_NAME,
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
    tasks_paths = []
    for page_no, task in tasks.items():
        output_path = os.path.join(tasks_dir, f"{page_no}.json")
        with open(output_path, "w") as f:
            json.dump(task, f, indent=4)
        tasks_paths.append(output_path)

    return tasks_paths


def _post_process_layouts(page_layouts, page_no):
    page_layouts = _iou(page_layouts)
    page_layouts = _semantic_transform(page_layouts, page_no)
    return page_layouts


def _iou(page_layouts, threshold=0.8):
    """
    Post-process the layout analysis results.
    It aimed to mitigate the problem of overlapping bounding boxes.
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

    def merge(a, b):
        """Merge two bounding boxes."""
        return min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3])

    def choose_region(first, second):
        _l1, _area1 = first
        _l2, _area2 = second

        # choose the one with higher confidence
        if _l1['conf'] > _l2['conf']:
            return _l1
        elif _l1['conf'] < _l2['conf']:
            return _l2
        # below if confidences are equal
        # then choose the one with the smaller area
        elif _area1 < _area2:
            return _l1
        elif _area1 > _area2:
            return _l2
        # below if confidence and areas are equal (but still can be various classes)
        # then choose the first one
        else:
            return _l1

    layouts = sorted(map(lambda x: (x, calculate_area(x['bbox'])), page_layouts), key=lambda x: x[1], reverse=True)
    i = 0

    while i < len(layouts):
        l1, area1 = layouts[i]
        j = i + 1

        while j < len(layouts):
            l2, area2 = layouts[j]

            # Calculate the intersection area of both regions
            inter_area = intersection_area(l1['bbox'], l2['bbox'])

            # Check if the intersection area of both regions is greater than the threshold
            ratio_inter_to_box1 = inter_area / area1
            ratio_inter_to_box2 = inter_area / area2
            if ratio_inter_to_box1 > threshold and ratio_inter_to_box2 > threshold:
                # Merge the two regions, because their intersection area is big to consider them as one region
                merged_bbox = merge(l1['bbox'], l2['bbox'])
                l = choose_region((l1, area1), (l2, area2))
                l['bbox'] = merged_bbox
                a = calculate_area(merged_bbox)
                #  remove old regions by their indexes and add the new merged region
                layouts = [x for idx, x in enumerate(layouts) if idx not in (i, j)] + [(l, a)]
                break
            # Check if one region is almost full inside the other
            elif ratio_inter_to_box1 > threshold or ratio_inter_to_box2 > threshold:
                # Leave only the most confident region and remove the other
                l = choose_region((l1, area1), (l2, area2))
                a = calculate_area(l['bbox'])
                layouts = [x for idx, x in enumerate(layouts) if idx not in (i, j)] + [(l, a)]
                break
            else:
                j += 1

        i += 1

    return [x[0] for x in layouts]


def _semantic_transform(page_layouts, page_no):
    """
    Post-process the layout analysis results based on knowledge how books typically look like.
    """
    prev_class = None
    for idx, l in enumerate(sorted(page_layouts, key=lambda x: (x["bbox"][1], x["bbox"][0]))):
        # replace all `titles` with `section-header` if the page number is greater than 1
        this_class = l["class"]
        if this_class == "title" and page_no > 1:
            l["class"] = "section-header"
        # if this class is `page-header` and it is not the first element on the page and previous element is not another
        # `page-header` then replace it with `text`
        elif this_class == 'page-header' and idx > 0 and prev_class != 'page-header':
            l["class"] = "text"
        elif this_class == 'list-item':
            l["class"] = "text"
    return page_layouts






