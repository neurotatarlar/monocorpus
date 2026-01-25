from google import genai
from google.genai import types
import time

    
def create_client(api_key):
    return genai.Client(api_key=api_key)


def gemini_api(prompt, model, client, files = {}, temperature=0.1, schema=None, timeout_sec=60*10):
    uploaded_files = []
    for path, mime_type in files.items():
        _f = upload_and_wait(client, path, mime_type)
        uploaded_files.append(_f)
    prompt.extend(uploaded_files)
    resp_stream = client.models.generate_content_stream(
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
    return resp_stream, uploaded_files


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