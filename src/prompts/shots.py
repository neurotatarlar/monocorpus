"""Prepare inline few-shot examples for Gemini prompts."""

import os
import base64
import json

ARTIFACTS_SHOTS_DIR = "_artifacts/shots"
LEGACY_SHOTS_DIR = "shots"
COOKED_SHOTS_DIRNAME = "cooked"
SNIPPETS_DIRNAME = "snippets"


def _resolve_shots_dir(subdir: str) -> str:
    """Return preferred shots subdir in _artifacts with legacy fallback."""
    preferred = os.path.join(ARTIFACTS_SHOTS_DIR, subdir)
    legacy = os.path.join(LEGACY_SHOTS_DIR, subdir)
    if os.path.exists(preferred) or not os.path.exists(legacy):
        return preferred
    return legacy

def load_inline_shots():
    """Return the prepared shots JSON, creating it if missing."""
    cooked_dir = _resolve_shots_dir(COOKED_SHOTS_DIRNAME)
    shots_file = os.path.join(cooked_dir, "prepared-shots.json")
    if not os.path.exists(shots_file):
        print("Creating new shots file")
        os.makedirs(cooked_dir, exist_ok=True)
        with open(shots_file, "w") as f:
            json.dump(_form_inline_shots(), f, ensure_ascii=False, indent=4)
    return shots_file

def _form_inline_shots(_dir = None):
    """Build the inline shots payload from snippets and images."""
    if _dir is None:
        _dir = _resolve_shots_dir(SNIPPETS_DIRNAME)
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
    """List files under a directory by suffix."""
    return [os.path.join(dir, f) for f in os.listdir(dir) if f.endswith(endswith)]
            
    
        
        
