from enum import Enum


class Dirs(Enum):
    ENTRY_POINT = "0_entry_point"
    CONTENT = "1_result"
    METADATA="2_metadata"
    DOC_SLICES = "misc/doc_slices"
    UPSTREAM_METADATA = "misc/upstream_metadata"
    PAGE_IMAGES = "misc/page_images"
    CLIPS = "misc/clips"
    CHUNKED_RESULTS = "misc/chunked_result"
    PROMPTS = "misc/prompts"
    WIPING_PLAN = "misc/wiping_plan"
    GEMINI_CLI_OUTPUT = "misc/gemini_cli_output"
