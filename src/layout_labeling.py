# import json
# import os
#
# from pymupdf import pymupdf
# from pymupdf.utils import getColor
# from rich.progress import track
#
# from consts import Dirs
# from file_utils import get_path_in_workdir
#
#
# def label_document(path_to_doc: str, path_to_la: str, md5: str):
#     with pymupdf.open(path_to_doc) as doc, open(path_to_la) as laf:
#         la = json.load(laf)
#         for page in doc:
#             page_no = str(page.number)
#
#             if page_no not in la:
#                 raise Exception(f"Page `{page_no}` not found in layout analysis")
#
#             shape = page.new_shape()
#             for layout in la[page_no]:
#                 box = layout["bbox"]
#                 text = ", ".join([f"{x['class']} {round(float(x['conf']), 2)} {x['id']}" for x in layout["layout"]])
#                 shape.draw_rect(box)
#                 color = get_color_by_class(layout["layout"][0]["class"])
#                 # insert text box slightly above the bbox
#                 x1, y1, _, _ = box
#                 shape.insert_text(
#                     (x1, y1 - 1),
#                     text,
#                     color=(0, 0, 0),
#                     fill=color,
#                     fontsize=12,
#                     border_width=0.03,
#                     render_mode=2,
#                 )
#                 shape.finish(color=color, width=0.1, fill=color, fill_opacity=0.2)
#             shape.commit(overlay=True)
#         doc.save(os.path.join(get_path_in_workdir(Dirs.LAYOUT_MARKED_DOCS), f"{md5}.pdf"))
#
#
# def get_color_by_class(cls: str):
#     match cls:
#         case "text":
#             return getColor("DarkOliveGreen")
#         case "picture":
#             return getColor("Salmon")
#         case "caption":
#             return getColor("Khaki")
#         case "section-header":
#             return getColor("Red")
#         case "footnote":
#             return getColor("Sienna")
#         case "formula":
#             return getColor("Coral")
#         case "table":
#             return getColor("Green")
#         case "list-item":
#             return getColor("DarkViolet")
#         case "page-header":
#             return getColor("Sienna")
#         case "page-footer":
#             return getColor("Blue")
#         case "title":
#             return getColor("Cyan")
#         case _:
#             raise Exception(f"Unknown class `{cls}`")
#
