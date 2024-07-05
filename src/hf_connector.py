import json
import os.path

import pymupdf
from huggingface_hub import hf_hub_download
from rich.progress import track
from ultralytics import YOLO

from consts import Dirs
from file_utils import get_path_in_workdir
from s3_uploader import upload_files_to_s3

REPO_ID = 'hantian/yolo-doclaynet'
MODEL_NAME = 'yolov9m'
MODEL_CHECKPOINT = f"{MODEL_NAME}-doclaynet.pt"
DPI = 300


def get_layout_analysis(path_to_file: str, md5: str):
    """
    Analyze the layout of the document

    First, the function makes images of the book pages.
    Then, it analyzes the layout of the pages and uploads the images and the layout analysis to S3.
    """
    # Make images of the book pages
    pages_details = _make_images_of_pages(path_to_file, md5)
    # Analyze the layout of the pages
    pages_details = _create_layout_analysis(pages_details, md5)
    # Upload the images to S3
    remote_files = upload_files_to_s3([f['path_to_image'] for f in pages_details.values()], f"images/{md5}")
    # Update the paths to the remote images
    for page_no, details in pages_details.items():
        details["remote_path"] = remote_files[details["path_to_image"]]

    # Export the predictions to the Label Studio format
    tasks_paths = _export_predictions(pages_details, md5)
    # Upload the task to S3
    upload_files_to_s3(tasks_paths, f"tasks/{md5}")


def _make_images_of_pages(path_to_file: str, md5: str):
    output_dir = os.path.join(get_path_in_workdir(Dirs.PAGE_IMAGES), md5)
    os.makedirs(output_dir, exist_ok=True)
    results = {}
    with pymupdf.open(path_to_file) as doc:
        for page in track(doc[:], description=f"Extracting images of the pages from `{md5}`..."):
            path_to_image = os.path.join(output_dir, f"{page.number}.jpg")
            if os.path.exists(path_to_image):
                pix = pymupdf.Pixmap(path_to_image)
            else:
                pix = page.get_pixmap(colorspace='rgb', alpha=False)
                pix.save(path_to_image, 'jpg')
            results[page.number] = {
                "path_to_image": path_to_image,
                "width": pix.width,
                "height": pix.height
            }
    return results


def _create_layout_analysis(pages_details, md5: str):
    model = YOLO(hf_hub_download(repo_id=REPO_ID, filename=MODEL_CHECKPOINT))

    for page_no, details in track(pages_details.items(), description=f"Analyzing layouts of the pages from `{md5}`..."):
        img = details["path_to_image"]
        pred = model(img, verbose=False)
        results = pred[0].cpu()
        boxes = results.boxes.xyxy.numpy()
        confs = results.boxes.conf.numpy()
        classes = results.boxes.cls.numpy()

        page_layouts = [
            {
                "bbox": x[0].tolist(),
                "layout": [{
                    "class": results.names[int(x[2])].lower(),
                    "conf": str(round(x[1], 2)),
                    "id": f"{page_no}::{idx}"
                }]
            }
            for (idx, x)
            in enumerate(zip(boxes, confs, classes))
        ]
        page_layouts = _merge_overlapping_boxes(page_layouts)
        details["layout"] = page_layouts

    return pages_details


def _merge_overlapping_boxes(page_layouts: list):
    def is_overlapping(a, b):
        return not (a[2] < b[0] or a[0] > b[2] or a[3] < b[1] or a[1] > b[3])

    def merge(a, b):
        return min(a[0], b[0]), min(a[1], b[1]), max(a[2], b[2]), max(a[3], b[3])

    for i in range(len(page_layouts)):
        for j in range(i + 1, len(page_layouts)):
            if is_overlapping(page_layouts[i]["bbox"], page_layouts[j]["bbox"]):
                page_layouts[i]["bbox"] = merge(page_layouts[i]["bbox"], page_layouts[j]["bbox"])
                layouts = page_layouts[i]["layout"]
                layouts.extend(page_layouts[j]["layout"])
                page_layouts[i]["layout"] = sorted(layouts, key=lambda x: x["conf"], reverse=True)
                page_layouts.pop(j)
                return _merge_overlapping_boxes(page_layouts)
    return page_layouts


def _export_predictions(pages_details, md5: str):
    total_pages = len(pages_details)
    tasks = {
        page_no: {
            "data": {
                "image": details["remote_path"],
                "page_no": page_no,
                "total_pages": total_pages,
                "hash": md5,
            },
            "predictions": [
                {
                    "model_version": MODEL_NAME,
                    "result": [
                        {
                            "id": f"{md5}::{layout["layout"][0]["id"]}",
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
                                    layout["layout"][0]["class"]
                                ],
                            }
                        }
                        for layout in sorted(details["layout"], key=lambda x: (x["bbox"][1], x["bbox"][0]))
                    ],
                }
            ]
        }
        for page_no, details
        in pages_details.items()
    }
    tasks_dir = get_path_in_workdir(Dirs.LABEL_STUDIO_TASKS)
    tasks_paths = []
    for page_no, task in tasks.items():
        output_path = os.path.join(tasks_dir, f"{page_no}_{md5}.json")
        with open(output_path, "w") as f:
            json.dump(task, f, indent=4)
        tasks_paths.append(output_path)

    return tasks_paths