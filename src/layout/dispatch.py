# First create a layout plan
# 1. extract layouts by yolo 
# 2. extract layouts by surya
# 3. create consensus layouts prediction
# try to create consensus, sort out simple layouts, find clips, separate to areas: 
# - all but first and last layout, last and first layouts mergedjoined 
#

# extract yolo
# extract syria 
# convert to intermediate format 
# compare and choose layout 
from utils import read_config, obtain_documents, download_file_locally, get_in_workdir
from monocorpus_models import Document, Session, SCOPES
from yadisk_client import YaDisk
from rich import print
from rich.progress import track
import os
from dirs import Dirs
import pymupdf
from huggingface_hub import hf_hub_download
from ultralytics import YOLO
from PIL import Image, ImageFilter, ImageEnhance
from surya.layout import LayoutPredictor
import json
    
    
# class PageImage:
    
#     def __init__(self, width, height, path):
#         self.width = width 
#         self.height = height
#         self.path = path

REPO_ID = 'hantian/yolo-doclaynet'
MODEL_NAME = 'yolov11l'
# MODEL_NAME = 'yolov12l'
MODEL_CHECKPOINT = f"{MODEL_NAME}-doclaynet.pt"
DPI = 100
SURYA_BATCH_SIZE=10

class Context:

    def __init__(self, doc):
        self.doc = doc
        self.local_path = None
        self.page_images = None
        self.surya_layouts = None 
        self.yolo_layouts = None
        
        

def layouts(cli_params):
    config = read_config()
    predicate = (
        # Document.content_url.is_(None) &
        Document.mime_type.is_("application/pdf") &
        Document.language.is_("tt-Cyrl") &
        Document.full.is_(True)
    )
    with YaDisk(config['yandex']['disk']['oauth_token']) as ya_client:
        with Session() as gsheets_session:
            docs = list(obtain_documents(cli_params, ya_client, predicate, limit=1, gsheet_session=gsheets_session))
            docs  = [Context(d) for d in docs]

        if not docs:
            print("No docs for layout detection, exiting...")
            return
        
        print(f"Got {len(docs)} docs for layout detection")
        for c in track(docs, "Downloading documents..."):
            c.local_path = download_file_locally(ya_client, c.doc, config)
            
        for c in docs:
            results = _page_images(c.local_path, c.doc.md5)
            c.page_images = results
            
        for c in docs:
            c.yolo_layouts = _inference_yolo_doclaynet(c)
            
        for c in docs:
            c.surya_layouts = _inference_surya(c)
            
        for c in docs:
            _render_bboxes(c)
        

def _render_bboxes(context):
    # take yolo layouts 
    with pymupdf.open(context.local_path) as pdf_doc:
        for d in context.yolo_layouts[:]:
            for page_no, details in d.items():
                page = pdf_doc[int(page_no)]
                width = details['w']
                height = details['h']
                for l in details['layouts']:
                    bbox = l['bbox']
                    x0 = bbox[0] * page.rect.width  / width
                    y0 = bbox[1] * page.rect.height / height
                    x1 = bbox[2] * page.rect.width  / width
                    y1 = bbox[3] * page.rect.height / height
                    rect = pymupdf.Rect(x0=x0, y0=y0, x1=x1, y1=y1)
                    page.draw_rect(rect, color=(1, 0, 0), width=1)  
                    
                    # add label
                    text = f"{l['class']}-{l['conf']}"
                    page.insert_text(
                        pymupdf.Point(x0, max(y0 - 1, 0)),
                        text,
                        fontsize=8,
                        color=(1, 0, 0),
                    )
                    
        # ---- Surya boxes (blue) ----
        for d in context.surya_layouts[:]:
            for page_no, details in d.items():
                page = pdf_doc[int(page_no)]
                width = details['w']
                height = details['h']
                for l in details['layouts']:
                    bbox = l['bbox']
                    x0 = bbox[0] * page.rect.width  / width
                    y0 = bbox[1] * page.rect.height / height
                    x1 = bbox[2] * page.rect.width  / width
                    y1 = bbox[3] * page.rect.height / height
                    rect = pymupdf.Rect(x0=x0, y0=y0, x1=x1, y1=y1)
                    page.draw_rect(rect, color=(0, 0, 1), width=1)  # blue box

                    fontsize = 8

                    # measure text width
                    text = f"{l['class']}-{l['conf']}-{l['pos']}"
                    text_width = pymupdf.get_text_length(text, fontsize=fontsize)
                    text_x = x1 - text_width
                    text_y = max(y0 - 1, 0)

                    page.insert_text(
                        pymupdf.Point(text_x, text_y),
                        text,
                        fontsize=fontsize,
                        color=(0, 0, 1),
                    )
        
        pdf_doc.save("1.pdf")
                

def _inference_yolo_doclaynet(context):
    yolo_predictions_path = get_in_workdir(Dirs.PREDICTIONS, file=f"yolo-doclaynet-{context.doc.md5}.json")
    
    if os.path.exists(yolo_predictions_path):
        print(f"Loading 'yolo-doclaynet' predictions by path '{yolo_predictions_path}'")
        with open(yolo_predictions_path, "r") as f:
            return json.load(f)

    model = YOLO(hf_hub_download(repo_id=REPO_ID, filename=MODEL_CHECKPOINT))

    layouts = []
    for page_no, paths_to_image in track(context.page_images.items(), description=f"Predicting layouts of the doc `{context.doc.md5}` by model 'yolo-doclaynet-{MODEL_NAME}'"):
        image = Image.open(paths_to_image['300'])
        pred = model.predict(image, verbose=False, imgsz=1024)
        results = pred[0].cpu()
        boxes = results.boxes.xyxy.numpy()
        confs = results.boxes.conf.numpy()
        classes = results.boxes.cls.numpy()

        page_layouts = [
            {
                "bbox": x[0].tolist(),
                "class": results.names[int(x[2])].lower(),
                "conf": str(round(x[1], 2)),
                # "id": f"{page_no}::{idx}"
            }
            for (idx, x)
            in enumerate(zip(boxes, confs, classes))
        ]
        # Save the plot
        # results.save(os.path.join(plots_dir, f"{page_no}.png"))

        layouts.append(
            {
                page_no: {
                    "layouts": page_layouts,
                    "w": image.width,
                    "h": image.height
                }
            }
        )

    with open(yolo_predictions_path, "w") as f:
        json.dump(layouts, f, ensure_ascii=False, indent=4)
        
    return layouts    


# {'Caption', 'Picture', 'PageFooter', 'TableOfContents', 'Equation', 'SectionHeader', 'Handwriting', 'Figure', 'PageHeader', 'Table', 'ListItem', 'Text'}
def _inference_surya(context):
    surya_predictions_path = get_in_workdir(Dirs.PREDICTIONS, file=f"surya-{context.doc.md5}.json")
    
    if os.path.exists(surya_predictions_path):
        print(f"Loading 'surya' predictions by path '{surya_predictions_path}'")
        with open(surya_predictions_path, "r") as f:
            return json.load(f)
    
    print(f"Predicting layouts of the doc `{context.doc.md5}` by model 'surya'")
    layout_predictor = LayoutPredictor()
    # for page_no, paths_to_image in track(context.page_images.items(), description=f"Analyzing layouts of the `{context.doc.md5}`..."):
    #     image = Image.open(paths_to_image["300"])

    #     # layout_predictions is a list of dicts, one per image
    #     layout_predictions = layout_predictor([image], top_k=1)
    #     for lp in layout_predictions: 
    #         print(lp)
    
    layouts = [] 
    images = [Image.open(p['300']) for _, p in context.page_images.items()]
    layout_predictions = layout_predictor(images[:], top_k=2, batch_size=SURYA_BATCH_SIZE)
    for idx, lp in enumerate(layout_predictions): 
        page_layouts = [
            {
                "class": b.label,
                "pos": b.position,
                "conf": str(round(b.confidence, 2)),
                "candidates": b.top_k,
                "bbox": b.bbox
            }
            for b
            in lp.bboxes 
        ]
        layouts.append(
            {
                idx: {
                    "layouts": page_layouts,
                    "w": images[idx].width,
                    "h": images[idx].height
                }
            }
        )
            
    with open(surya_predictions_path, "w") as f:
        json.dump(layouts, f, ensure_ascii=False, indent=4)
        
    return layouts
            

    
def _page_images(path_to_file: str, md5: str):
    output_dir = os.path.join(get_in_workdir(Dirs.PAGE_IMAGES), md5)
    os.makedirs(output_dir, exist_ok=True)
    results = {}
    with pymupdf.open(path_to_file) as doc:
        for page in track(list(doc.pages(stop=30)), description=f"Extracting images of the doc '{md5}'..."):
            path_to_image_100_dpi = os.path.join(output_dir, f"page_{page.number}_100_dpi.png")
            if not os.path.exists(path_to_image_100_dpi):
                pix = page.get_pixmap(colorspace='rgb', alpha=False, dpi=100)
                image = pix.pil_image()
                image = image.convert("L")
                image = image.filter(ImageFilter.MedianFilter(size=3))
                enhancer = ImageEnhance.Contrast(image)
                image = enhancer.enhance(2.0) 
                enhancer = ImageEnhance.Brightness(image)
                image = enhancer.enhance(1.1) 
                enhancer = ImageEnhance.Sharpness(image)
                image = enhancer.enhance(1.5)                 
                image.save(path_to_image_100_dpi, 'png')
                # pix.save(path_to_image_100_dpi, 'png')
                
            path_to_image_300_dpi = os.path.join(output_dir, f"page_{page.number}_300_dpi.png")
            if not os.path.exists(path_to_image_300_dpi):
                pix = page.get_pixmap(colorspace='rgb', alpha=False, dpi=300)
                image = pix.pil_image()
                image = image.convert("L")
                image = image.filter(ImageFilter.MedianFilter(size=3))
                enhancer = ImageEnhance.Contrast(image)
                image = enhancer.enhance(2.0) 
                enhancer = ImageEnhance.Brightness(image)
                image = enhancer.enhance(1.1) 
                image.save(path_to_image_300_dpi, 'png')
                # pix.save(path_to_image_300_dpi, 'png')
                
            # path_to_image_500_dpi = os.path.join(output_dir, f"page_{page.number}_500_dpi.png")
            # if not os.path.exists(path_to_image_500_dpi):
            #     pix = page.get_pixmap(colorspace='rgb', alpha=False, dpi=500)
            #     pix.save(path_to_image_500_dpi, 'png')
                
            results[page.number] = {
                "100": path_to_image_100_dpi,
                "300": path_to_image_300_dpi,
                # "500": path_to_image_500_dpi,
            }
    return results

