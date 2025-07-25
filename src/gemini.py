from google import genai
from google.genai import types
import subprocess
import os
from utils import workdir
    
def gemini_cli(config, prompt):
    env = os.environ.copy()
    env["GEMINI_API_KEY"] = config['google_api_key']['free']
    
    command = [
        os.path.abspath(os.path.join(os.path.expanduser("~"), ".npm-global/bin/gemini")),
        "--model", "gemini-2.5-pro",
        "--prompt", prompt,
        "--yolo",
    ]
    try:
        output = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            env=env,
            cwd=os.path.abspath(os.path.expanduser(workdir))
        )
        output = output.stdout.strip()
        print("Gemini cli output ==> ", output)
        if "gemini-2.5-flash" in output:
            raise ValueError("Request was executed by flash model")
        elif "overloaded" in output:
            raise ValueError("Model is overloaded")
        return output
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr.strip()}")
        raise e

def create_client(api_key):
    return genai.Client(api_key=api_key)

def gemini_api(prompt, model, client, files = {}, temperature=0.1, schema=None, timeout_sec=60*10):
    for path, mime_type in files.items():
        _f = client.files.upload(
            file=path,
            config={"mime_type": mime_type}
        )
        prompt.append(_f)
    return client.models.generate_content_stream(
        model=model,
        contents=prompt,
        # docs https://ai.google.dev/gemini-api/docs/text-generation#configuration-parameters
        config=types.GenerateContentConfig(
            temperature=temperature,
            response_mime_type="application/json",
            response_schema=schema,
            candidate_count=1,
            seed=1552,
            http_options=types.HttpOptions(
                timeout=timeout_sec * 1000
            )
        )
    )
