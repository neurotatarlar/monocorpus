from utils import read_config
from google import genai
from google.genai import types
import time

def create_client(api_key):
    return genai.Client(api_key=api_key)

def request_gemini(prompt, model, client, files = {}, temperature=0.1, schema=None, timeout_sec=60*10):
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
