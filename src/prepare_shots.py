import os
from PIL import Image
import json
import base64

inline_shots_path = "./shots/inline-shots.json"

def prepare_shots():
    # _resize()
    inline_shots = _form_inline_shots()
    with open(inline_shots_path, 'w') as f:
        json.dump(inline_shots, f, ensure_ascii=False, indent=None, separators=(',', ':'))

def _resize(target_size=1536):
    
    for img_path in _list_files('./shots', enswith='.png'):
        img = Image.open(img_path)
        width, height = img.size
        
        if width > height:
            scale_factor = target_size / width
        else:
            scale_factor = target_size / height
        
        if scale_factor < 1:
            new_width = int(width * scale_factor)
            new_height = int(height * scale_factor)
        
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
            img.save(img_path)
            
def _form_inline_shots(_dir = './shots'):
    prompt = [{"text": "Here are examples of how to extract content from a document. Additionally these examples illustrate how to correctly handle paragraphs and tables that continue across pages in this chunk. Follow these conventions to ensure structural continuity and preserve reading flow."}]
    
    for idx, ground_truth_path in enumerate(_list_files(_dir, enswith='.md'), start=1):
        _id, _ = os.path.splitext(os.path.basename(ground_truth_path))
        _id = _id[:-1]
        
        with open(os.path.join(_dir, f"{_id}1.png"), 'rb') as f:
            prev_img = base64.b64encode(f.read()).decode("utf-8")
            prompt.append({"text": f"Example {idx} Previous page"})
            prompt.append({"inline_data": {
                "data": prev_img,
                "mime_type": "image/png",
            }})
            
        with open(os.path.join(_dir, f"{_id}2.png"), 'rb') as f:
            cur_img = base64.b64encode(f.read()).decode("utf-8")
            prompt.append({"text": f"Example {idx} Current page"})
            prompt.append({"inline_data": {
                "data": cur_img,
                "mime_type": "image/png",
            }})
            
        with open(ground_truth_path, 'r', encoding='utf-8') as f:
            prompt.append({"text": f"✅ Example {idx} Ground Truth in Markdown format:\n{f.read()}"})
                
        # footnotes = os.path.join(_dir, f"{_id}3.json")
        # if os.path.exists(footnotes):
        #     with open(footnotes, 'r', encoding='utf-8') as f:
        #         prompt.append({"text": f"Example {idx} Formatted footnotes on the page:\n{f.read()}"})
        # else:
            # prompt.append({"text": f"Example {idx} has no Formatted footnotes on the page"})
            
    prompt.append({"text": "Summary: \n- **Do not** insert a blank line if the paragraph is continuing. Merge seamlessly and naturally\n -**Do not** restart the table if it continues from a previous page. Just append the rows inside the same block."})    
    return prompt  

def _list_files(dir, enswith):
    return [os.path.join(root, f) for root, _, files in os.walk(dir) for f in files if f.endswith(enswith)]

def load_inline_shots():
    if not os.path.exists(inline_shots_path):
        prepare_shots()
    with open(inline_shots_path, 'r') as f:
        return json.load(f)
    
            
    
        
        
