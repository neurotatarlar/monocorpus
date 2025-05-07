from utils import get_in_workdir
from dirs import Dirs
from itertools import batched
import pymupdf
from prompt import EXTRACT_CONTENT_PROMPT, PREV_CHUNK_TAIL_PROMPT
import zipfile
from gemini import request_gemini, create_client
from schema import ExtractionResult
import mdformat
import re
from prepare_shots import load_inline_shots
from bs4 import BeautifulSoup
import json
from collections import defaultdict
import os
from huggingface_hub import hf_hub_download
from ultralytics import YOLO
from PIL import Image, ImageDraw
import math
import fitz
from s3 import upload_file, create_session

REPO_ID = 'hantian/yolo-doclaynet'
MODEL_NAME = 'yolov10b'
MODEL_CHECKPOINT = f"{MODEL_NAME}-doclaynet.pt"

# Content extraction checklist:
# - book in Tatar language
# - book does not have special chars, eg diacritic
# - all tables are oriented horizontally
# - all pages aligned horizontally

# todo postprocess output: remove page delimeters, fix headers hierarchy
# todo more shots: between pages, footnotes
# todo preview returned markdown
# todo upload inline shots in advance?
# alt text for image?
def extract(context):
    client = create_client("promo")
    with pymupdf.open(context.local_doc_path) as pdf_doc:
        _extract_content(context, pdf_doc, client)
        context.extraction_method = f"{context.cli_params.model}/{context.cli_params.batch_size}/pdfinput"
        context.doc_page_count=pdf_doc.page_count

def _extract_content(context, pdf_doc, client):
    batch_size = context.cli_params.batch_size
    iter = list(batched(range(0, pdf_doc.page_count)[context.cli_params.page_slice], batch_size))

    context.formatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-formatted.md")
    unformatted_response_json = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.json")
    unformatted_response_md = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}-unformatted.md")
    prev_chunk_tail = None
    next_footnote_num = 1
    
    with open(context.formatted_response_md , "w", encoding="utf-8") as formatted, open(unformatted_response_json , "w", encoding="utf-8") as unformatted_json, open(unformatted_response_md , "w", encoding="utf-8") as unformatted_md:
        unformatted_json.write("[")
        for idx, batch in enumerate(context.progress.track_extraction(iter, f"Extracting content in batches of size '{batch_size}'"), start=1):
            # create a pdf doc what will contain a slice of original pdf doc
            slice_from = batch[0]
            slice_to = batch[-1]
            doc_slice = pymupdf.open()
            doc_slice.insert_pdf(pdf_doc, from_page=slice_from, to_page=slice_to)
            slice_file_path = get_in_workdir(Dirs.DOC_SLICES, context.md5, file=f"slice-{slice_from}-{slice_to}.pdf")
            doc_slice.save(slice_file_path)
            
            # prepare prompt
            prompt = _prepare_prompt(slice_from, slice_to, next_footnote_num, prev_chunk_tail)
            
            # request gemini
            files = {slice_file_path: "application/pdf"}
            response = request_gemini(client=client, model=context.cli_params.model, prompt=prompt, files=files, schema=ExtractionResult)
            
            raw_content = ''
            for chunk in response:
                if text := chunk.text:
                    raw_content += text
                    
            # write raw response in json 
            unformatted_json.write(raw_content)
            if idx < len(iter):
                unformatted_json.write(",\n")
            
            # validate response 
            extraction_result = ExtractionResult.model_validate_json(raw_content)
            
            # write down variant before preprocessing for debugging and observability purpose 
            unformatted_md.write(extraction_result.content)
            
            # postprocess response
            formatted_content, last_footnote_num, = _post_process(extraction_result)
            next_footnote_num = max(next_footnote_num, last_footnote_num + 1)
            prev_chunk_tail = extraction_result[:200]
            
            # write down processed result
            formatted.write(formatted_content)
            
            context.tokens.append(chunk.usage_metadata.total_token_count)
        formatted.write('\n\n---\n')
        unformatted_json.write("]")

    mdformat.file(
        context.formatted_response_md,
        codeformatters=(),
        extensions=["toc", "footnote"],
        options={"wrap": "keep", "number": "keep", "validate": True, "end_of_line": "lf"},
    )

    context.local_content_path = get_in_workdir(Dirs.CONTENT, file=f"{context.md5}.zip")
    with zipfile.ZipFile(context.local_content_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        zf.write(arcname=f"{context.md5}.md", filename=context.formatted_response_md)

def _prepare_prompt(slice_from, slice_to, footnote_counter, prev_chunk_tail=None):
    prompt = [{"text": EXTRACT_CONTENT_PROMPT.format(slice_to=slice_to, slice_from=slice_from, footnote_start=footnote_counter).strip()}]
    print("slice_from", slice)
    if slice_from:
        # does not contain document title, so all headers should be '##' or deeper
        prompt.append({"text": "📌 Document does not have a title page, so use ## for the highest-level headings, ### for subsections, and so on. Never use a single #. Always preserve the heading hierarchy based on the document's logical structure."})
    else:
        # may contain document title
        prompt.append({"text": "📌 Document may contain a main title. If you detect a main document title mark it with a single #. Use ## for top-level sections, ### for subsections, and so on. Always preserve the heading hierarchy based on the document's logical structure."})
    
    print("prev_chunk_tail", prev_chunk_tail)
    if prev_chunk_tail:
        prompt.append({"text": PREV_CHUNK_TAIL_PROMPT.strip()})
        prompt.append({"text": prev_chunk_tail})
        
    prompt.extend(load_inline_shots())
    prompt.append({"text": "Now, extract structured content from the following document"})
    
    return prompt

def _post_process(context, extraction_result):
    postprocessed = extraction_result.replace("\\n", "\n").replace('-\n', '')
    # replace detected TOC with marker for mdformat-toc.
    # it signalizes mdformat-toc to create TOC based on headers in the document 
    postprocessed = re.sub(r'<table\s+class="toc">.*?</table>','<!-- mdformat-toc start --no-anchors -->', postprocessed, flags=re.DOTALL)
    
    # define number of last footnote detected in the document
    footnote_counters = re.findall(r"\[\^(\d+)\]:", postprocessed)
    last_footnote_counter = max(map(int, footnote_counters)) if footnote_counters else 0
    
    # exctract images
    postprocessed = _proccess_images(context, postprocessed)
    
    return postprocessed, last_footnote_counter

def _proccess_images(context, content):
    dashboard = _collect_images(content)
    if not dashboard:
        return content
    
    result = []
    images_dir = get_in_workdir(Dirs.PAGE_IMAGES, context.md5)
    clips_dir = get_in_workdir(Dirs.CLIPS)
    model = YOLO(hf_hub_download(repo_id=REPO_ID, filename=MODEL_CHECKPOINT))
    session = create_session(context.config)
    with pymupdf.open(context.local_doc_path) as doc:
        for page_no, details in dashboard.items():
            page = doc[page_no]
            path_to_page_image = os.path.join(images_dir, f"{page.number}-orig.png")
            if os.path.exists(path_to_page_image):
                pix = pymupdf.Pixmap(path_to_page_image)
            else:
                pix = page.get_pixmap(colorspace='rgb', alpha=False, dpi=300)
                pix.save(path_to_page_image, 'png')
                
            pred = model.predict(path_to_page_image, verbose=False, imgsz=1024, classes=[6]) #picture
            _results = pred[0].cpu()
            boxes = _results.boxes.xyxy.numpy()
            confs = _results.boxes.conf.numpy()
            classes = _results.boxes.cls.numpy()
            detected_images = [
                {
                    'bbox': x[0].tolist(), # coordinates are absolute to page size
                    "conf": str(round(x[1], 2)),
                    "class": _results.names[int(x[2])].lower(),
                }
                for x
                in zip(boxes, confs, classes)
            ]
            assert all(d['class'] == 'picture' for d in detected_images), "Some of detected layouts are not pictures"
            details['yolo'] = detected_images
                
            # draw bboxes on image
            boxed_image = pix.pil_image()
            draw = ImageDraw.Draw(boxed_image)
            
            for d in details['yolo']:
                draw.rectangle(d['bbox'], outline="green", width = 10)
            
            width, height = boxed_image.size
            for d in details['gemini']:
                y0, x0, y1, x1 = d['bbox']
                x0 = x0 / 1000 * width
                y0 = y0 / 1000 * height
                x1 = x1 / 1000 * width
                y1 = y1 / 1000 * height
                d['bbox'] = [x0, y0, x1, y1]
                draw.rectangle(d['bbox'], outline="red", width = 10)
            
            path_to_page_image_boxed = os.path.join(images_dir, f"{page.number}-boxed.png")
            boxed_image.save(path_to_page_image_boxed, format = 'png')
            pairs = _pair_model_boxes(details, centroid_distance_threshold = (width + height) / 10)
            _clips(pix, pairs, page_no, clips_dir, context.md5)
            _upload_to_s3(pairs, session, context)
            _compile_replacement_str(pairs)
            result.extend([(p['gemini']['html'], p['replacement']) for p in pairs])
    
    return _replace_images(result, content)
    

def _replace_images(result, content):
    for target, replacement in result:
        content = content.replace(target, replacement)
    return content

def _upload_to_s3(pairs, session, context):
    for p in pairs:
        path = p['path']
        bucket = context.config["yandex"]["cloud"]['bucket']['image']
        key = os.path.basename(path)
        p['url'] = upload_file(path, bucket, key, session)

def _clips(pix, pairs, page_no, clips_dir, md5):
    image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    for idx, p in enumerate(sorted(pairs, key=lambda f: (f["yolo"]["bbox"][0], f["yolo"]["bbox"][1]))):
        p['path'] = os.path.join(clips_dir, f"{md5}-{page_no}-{idx}.png")
        cropped_image = image.crop(p['yolo']['bbox'])
        cropped_image.save(p['path'], 'png')
        p['width'] = cropped_image.width
        p['height'] = cropped_image.height

def _compile_replacement_str(pairs):
    for p in pairs:
        if caption := p['gemini'].get('caption'):
            caption = f"<figcaption>{caption}</figcaption>"
        p['replacement'] = f'<figure><img alt="" src="{p['url']}" width="{int(p['width']/2)}" height="{int(p['height']/2)}">{caption or ''}</figure>'
    
def _pair_model_boxes(details, centroid_distance_threshold, iou_threshold = 0.5):
    matches = []
    for gem_box in details['gemini']:
        best_iou = 0
        best_yolo = None
        for yolo_box in details['yolo']:
            iou = compute_iou(gem_box['bbox'], yolo_box['bbox'])
            if iou > best_iou:
                best_iou = iou
                best_yolo = yolo_box

        if best_iou > iou_threshold:
            matches.append({
                'gemini': gem_box,
                'yolo': best_yolo,
                'method': 'iou',
                'score': best_iou
            })
        else:
            # Fallback: match by closest centroid
            gem_centroid = compute_centroid(gem_box['bbox'])
            min_dist = float('inf')
            closest_yolo = None
            for yolo_box in details['yolo']:
                yolo_centroid = compute_centroid(yolo_box['bbox'])
                dist = compute_distance(gem_centroid, yolo_centroid)
                if dist < min_dist:
                    min_dist = dist
                    closest_yolo = yolo_box

            if min_dist < centroid_distance_threshold:
                matches.append({
                    'gemini': gem_box,
                    'yolo': closest_yolo,
                    'method': 'centroid distance',
                    'score': min_dist
                })
            else:
                print("No matching pair found")
    return matches


def _collect_images(content):
    pattern = re.compile(r'(<figure.*?</figure>)', re.DOTALL)
    dashboard = defaultdict(dict)
    for match in pattern.finditer(content):
        raw_html = match.group(1)
        fig_elem = BeautifulSoup(raw_html, 'html.parser').find('figure')
        details = {
            'html': raw_html,
            'bbox': json.loads(fig_elem.get("data-bbox")),
        }
        if caption := fig_elem.find("figcaption"):
            details['caption'] = caption.get_text(strip=True)
            
        page_no = int(fig_elem.get("data-page"))
        if not dashboard[page_no].get('gemini'):
            dashboard[page_no]['gemini'] = []
        dashboard[page_no]['gemini'].append(details)
    return dashboard

def compute_iou(box1, box2):
    xa = max(box1[0], box2[0])
    ya = max(box1[1], box2[1])
    xb = min(box1[2], box2[2])
    yb = min(box1[3], box2[3])

    inter_area = max(0, xb - xa) * max(0, yb - ya)
    box1_area = (box1[2] - box1[0]) * (box1[3] - box1[1])
    box2_area = (box2[2] - box2[0]) * (box2[3] - box2[1])

    union_area = box1_area + box2_area - inter_area
    return inter_area / union_area if union_area > 0 else 0

def compute_centroid(box):
    x1, y1, x2, y2 = box
    return ((x1 + x2) / 2, (y1 + y2) / 2)

def compute_distance(c1, c2):
    return math.sqrt((c1[0] - c2[0]) ** 2 + (c1[1] - c2[1]) ** 2)