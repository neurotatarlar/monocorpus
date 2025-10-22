from google import genai
from google.genai import types
import subprocess
import os
from utils import workdir
import time

    
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
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            env=env,
            cwd=os.path.abspath(os.path.expanduser(workdir))
        )
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr.strip()}")
        raise e

def create_client(api_key):
    return genai.Client(api_key=api_key)

def gemini_api(prompt, model, client, files = {}, temperature=0.1, schema=None, timeout_sec=60*10):
    for path, mime_type in files.items():
        # _f = client.files.upload(
        #     file=path,
        #     config={"mime_type": mime_type}
        # )
        _f = upload_and_wait(client, path, mime_type)
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
            ),
        )
    )

def upload_and_wait(client, path, mime_type, poll_interval=0.3, timeout=10):
    _f = client.files.upload(file=path, config={"mime_type": mime_type})
    waited = 0
    while True:
        f_state = client.files.get(name=_f.name)
        if f_state.state == "ACTIVE":
            return f_state
        time.sleep(poll_interval)
        waited += poll_interval
        if waited >= timeout:
            raise TimeoutError(f"File {path} did not become ACTIVE in time.")