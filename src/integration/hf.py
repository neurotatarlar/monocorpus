# import os
#
# from consts import Dirs
# from file_utils import get_path_in_workdir
# import pandas as pd
#
# def upload():
#     upload_candidates = {}
#     for dirpath, dirnames, filenames in os.walk(get_path_in_workdir(Dirs.ARTIFACTS)):
#         for f in filenames:
#             if not f.endswith(".md"):
#                 continue
#
#             with open(os.path.join(dirpath, f), "r") as _f:
#                 upload_candidates[f[:-3]] = _f.read()
#
#     df = pd.DataFrame.from_dict(upload_candidates.items(), columns = ["key", "text"], orient="index")
#     print(df.head(5))
#
#
