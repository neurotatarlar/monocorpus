import os
import base64
import json

cooked_shots_dir = "./shots/cooked"

def load_inline_shots():
    shots_file = os.path.join(cooked_shots_dir, f"prepared-shots.json")
    if not os.path.exists(shots_file):
        print("Creating new shots file")
        with open(shots_file, "w") as f:
            json.dump(_form_inline_shots(), f, ensure_ascii=False, indent=4)
    return shots_file

def _form_inline_shots(_dir = './shots/snippets'):
    prompt = ["Here are examples of how to extract content from a document:"]
    gt = _list_files(_dir, endswith='.md')
    for idx, ground_truth_path in enumerate(gt, start=1):
        _id, _ = os.path.splitext(os.path.basename(ground_truth_path))
        _id = _id[:-1]
        
        with open(os.path.join(_dir, f"{_id}1.jpeg"), 'rb') as f:
            img = base64.b64encode(f.read()).decode("utf-8")
            prompt.append(f"Example {idx} Image:")
            prompt.append({"inline_data": {
                "data": img,
                "mime_type": "image/jpeg",
            }})
            
        with open(ground_truth_path, 'r', encoding='utf-8') as f:
            prompt.append(f"✅ Example {idx} Ground Truth: ```markdown\n{f.read()}\n```")
             
    return prompt  

def _list_files(dir, endswith):
    return [os.path.join(dir, f) for f in os.listdir(dir) if f.endswith(endswith)]
            
    
        
        
