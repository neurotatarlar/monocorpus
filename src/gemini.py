from utils import read_config
from google import genai
from google.genai import types
from prompt import SYSTEM_PROMPT

def create_client(config=read_config()):
    return genai.Client(api_key=config['google_api_key'])

def request_gemini(prompt, files = {}, client=create_client(), model='gemini-2.5-flash-preview-04-17', temperature=0.1, schema=None, response_mime_type=None):
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
            system_instruction=SYSTEM_PROMPT.strip(),
            temperature=temperature,
            response_mime_type=response_mime_type if response_mime_type else "application/json" if schema else None,
            response_schema=schema,
            candidate_count=1,
            seed=1552,
        )
    )

