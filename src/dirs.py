from enum import Enum


class Dirs(Enum):
    ENTRY_POINT = "0_entry_point"
    DOC_SLICES = "misc/doc_slices"
    CONTENT = "1_result"
    METADATA="2_metadata"
    UPSTREAM_METADATA = "misc/upstream_metadata"
    PAGE_IMAGES = "misc/page_images"
    CLIPS = "misc/clips"
