# from s3 import download
# from utils import read_config, get_in_workdir
# from dirs import Dirs
# from rich import print 
# from rich.progress import track
# import zipfile
# import json
# import os
# from monocorpus_models import Document, Session, SCOPES
# import regex
# from rapidfuzz import fuzz, process
# import numpy as np
# from typing import Optional
# from pydantic import BaseModel, Field
# from typing import Set
# import typer
# from rapidfuzz.distance import Levenshtein
# from sklearn.cluster import AgglomerativeClustering
# import numpy as np
# from collections import defaultdict


# # check surname endwith '-на', цкая must be кызы, father name must be ovna, surname endswith Константин
# # todo normalize Yanalif
# # problem with yo omitting

# # example matches: Г. Тукай, Р.А. Усманов, 
# pattern_surname_last_with_initials_first = regex.compile(
#     r'^(?P<name_initial>[\p{IsCyrillic}&&\p{Lu}])\.\s*'                # name initial
#     r'(?:(?P<father_initial>[\p{IsCyrillic}&&\p{Lu}])\.\s*)?'          # optional father’s initial
#     r'(?P<surname>[\p{IsCyrillic}&&\p{Lu}][\p{IsCyrillic}&&\p{Ll}]+'   # surname first part
#     r'(?:-[\p{IsCyrillic}&&\p{Lu}][\p{IsCyrillic}&&\p{Ll}]+)*)$'       # optional hyphen parts
# )

# # example matches: Тукай Г., Усманов Р.А.
# pattern_surname_first_with_initial_last = regex.compile(
#     r'^(?P<surname>[\p{IsCyrillic}&&\p{Lu}][\p{IsCyrillic}&&\p{Ll}]+'   # surname first part
#     r'(?:-[\p{IsCyrillic}&&\p{Lu}][\p{IsCyrillic}&&\p{Ll}]+)*)'         # optional hyphen parts
#     r',?'                                                               # optional comma
#     r'\s+'                                                              # mandatory whitespace
#     r'(?P<name_initial>[\p{IsCyrillic}&&\p{Lu}])\.'                     # first initial + dot
#     r'\s*'                                                              # optional space
#     r'(?:(?P<father_initial>[\p{IsCyrillic}&&\p{Lu}])\.?)?$'              # optional second initial + dot
# )

# # example matches: Җәлилова Йолдыз, Әхмәдуллина Рәмзилә Фирдинатовна
# pattern_fullname_surname_first = regex.compile(
#     r'^(?P<surname>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+(?i:ов|ова|ев|ева|ин|ина|нко|ская|ский|ко|цкая|цкий))\s+'  # surname
#     r'(?P<name>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+)'                                   # given name
#     r'(?:\s+(?P<father_name>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+))?$'                        # optional father’s name
# )

# # example matches: Чулпан Мухаррамовна Харисова, Әхмәт Халидов
# pattern_fullname_surname_last = regex.compile(
#     r'^(?P<name>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+)\s+'                           # given name
#     r'(?:(?P<father_name>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+)\s+)?'               # optional father's name
#     r'(?P<surname>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+(?:-(?:[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+))?'  # optional hyphen
#     r'(?i:ов|ова|ев|ева|ин|ина|нко|ская|ский|ко|цкая|цкий))$'                                         # mandatory ending
# )

# # example matches: Әминә Галимардан кызы Шайхулова
# pattern_tatar_father_marker_first = regex.compile(
#     r'^(?P<name>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+)\s+'                     # given name
#     r'(?P<father_name>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+)\s+'                # father's name
#     r'(?P<father_marker>улы|углы|кызы)\s+'                           # mandatory father marker
#     r'(?P<surname>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+(?:-(?:[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+))?'  # optional hyphen
#     r'(?i:ов|ова|ев|ева|ин|ина|нко|ская|ский|ко|цкая|цкий))$'                                    # mandatory ending
# )

# # example match: Әхмәтова Динара Равил кызы, Халиков Хәкимҗан Шәяхмәт улы
# pattern_tatar_father_marker_last = regex.compile(
#     r'^(?P<surname>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+(?:-(?:[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+))?'  # surname (optional hyphen)
#     r'(?i:ов|ова|ев|ева|ин|ина|нко|ская|ский|ко|цкая|цкий))\s+'                                                  # mandatory ending
#     r'(?P<name>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+)'                                         # given name
#     r'(?:\s+(?P<father_name>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+))?'                           # optional father's name
#     r'(?:\s+(?P<father_marker>улы|углы|кызы))?$'                                     # optional father's marker at the end
# )

# pattern_second_word_is_father_name = regex.compile(
#     r'^(?P<name>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+)\s+'                      # first name
#     r'(?P<father_name>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+(?i:ович|овна|евич|евна))\s+'  # patronymic
#     r'(?P<surname>[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+(?:-[А-ЯӘӨҮҖҢҺЁа-яәөүҗңһё]+)*)$'  # surname (hyphen allowed)
# )

# normalized_persons_path = "_artifacts/normalized_persons.json"
# raw_persons_file = '_artifacts/raw_persons.csv'
# raw_publishers_file = '_artifacts/raw_publishers.csv'


# def normalize():
#     if os.path.exists(normalized_persons_path):
#         with open(normalized_persons_path, "r") as f:
#             existing_persons = set([Person(**p) for p in json.load(f)])
#     else:
#         existing_persons = set()
        
#     print(f"Loaded {len(existing_persons)} existing persons from {normalized_persons_path}")
#     all_names = set()
#     for p in existing_persons:
#         if p.patronymic_marker == 'улы':
#             continue
#         all_names.add(p.firstname)
#     for n in sorted(list(all_names)):
#         print(f"{n}")

# def cluster_relatives(persons):
#     all_names = set()
#     for p in persons:
#         all_names.add(p.firstname)
#         all_names.add(p.fathername)
        
#     # Precompute distance matrix
#     n = len(all_names)
#     dist_matrix = np.zeros((n, n))
    
#     all_names = list(all_names)
#     for i in range(n):
#         for j in range(i+1, n):
#             dist = 100 - fuzz.token_sort_ratio(all_names[i], all_names[j])
#             dist_matrix[i, j] = dist
#             dist_matrix[j, i] = dist

#     # Agglomerative clustering
#     clustering = AgglomerativeClustering(
#         n_clusters=None, 
#         metric='precomputed', 
#         linkage='complete', 
#         distance_threshold=15  # lines with >=85 similarity are clustered together
#     )
    
#     labels = clustering.fit_predict(dist_matrix)

#     # Group lines by cluster
#     clusters = defaultdict(list)
#     for label, all_names in zip(labels, all_names):
#         clusters[label].append(all_names)

#     cluster_memebers = [members for members in clusters.values() if len(members) > 1]
#     print(f"Found {len(cluster_memebers)} clusters")
#     for members in cluster_memebers:
#         if len(members) > 1:
#             print("==================================================================")
#             print(f"Cluster members: f{members}")
    
# #     if not (os.path.exists(raw_persons_file) or os.path.exists(raw_publishers_file)):
# #         _prepare_raw_materials(raw_persons_file, raw_publishers_file)
        
# #     new_persons = _create_persons(raw_persons_file)
# #     full_name_persons = [p for p in new_persons if p.fathername]
    
# #     merged_persons = sorted(_merged_persons(existing_persons, full_name_persons))
    
# #     try:
# #         cluster_relatives(merged_persons)
# #     except KeyboardInterrupt:
# #         print("Interrupted by user, saving progress...")
# #     finally:
# #         with open(normalized_persons_path, "w") as f:
# #             json.dump([p.dict() for p in merged_persons], f, ensure_ascii=False, indent=4)
            
            
# # def _merged_persons(existing_persons, new_persons):
# #     aliases_to_person = {}
# #     for p in existing_persons:
# #         for alias in p.aliases:
# #             aliases_to_person[alias] = p
            
# #     all_persons = set(existing_persons)
# #     for np in new_persons:
# #         for alias in np.aliases:
# #             if alias in aliases_to_person:
# #                 break
# #         else:
# #             all_persons.add(np)
# #             for alias in np.aliases:
# #                 aliases_to_person[alias] = np
# #     return all_persons
        

# # def cluster_relatives(persons):
# #     # Precompute distance matrix
# #     n = len(persons)
# #     dist_matrix = np.zeros((n, n))
    
# #     persons_list = list(persons)
# #     for i in range(n):
# #         for j in range(i+1, n):
# #             dist = 100 - fuzz.token_sort_ratio(persons_list[i].compare_name(), persons_list[j].compare_name())
# #             dist_matrix[i, j] = dist
# #             dist_matrix[j, i] = dist

# #     # Agglomerative clustering
# #     clustering = AgglomerativeClustering(
# #         n_clusters=None, 
# #         metric='precomputed', 
# #         linkage='complete', 
# #         distance_threshold=15  # lines with >=85 similarity are clustered together
# #     )
    
# #     labels = clustering.fit_predict(dist_matrix)

# #     # Group lines by cluster
# #     clusters = defaultdict(list)
# #     for label, person in zip(labels, persons):
# #         clusters[label].append(person)

# #     cluster_memebers = [members for members in clusters.values() if len(members) > 1]
# #     print(f"Found {len(cluster_memebers)} clusters")
# #     for members in cluster_memebers:
# #         if len(members) > 1:
# #             # print("Choose the best variant from the following:")
# #             # for num, m in enumerate(members):
# #             #     print(f"{num} => {m})") 
# #             # choice = input(f"Cluster {cl} has {len(members)} members. Enter the number of the best variant (or 's' to skip): ")
# #             print("==================================================================")
# #             for num, m in enumerate(members, start=1):
# #                 print(f"{num} => {m})") 
# #             choice = typer.prompt(f"Choose the best variant")
            
# #             try:
# #                 chosen_variant = members[int(choice)-1]
# #                 for m in members:
# #                     if m != chosen_variant:
# #                         chosen_variant.consume(m)
# #                         persons.remove(m)
# #                 print(f"You chose: {choice}: {chosen_variant}")
# #             except (ValueError, IndexError):
# #                 print("Invalid choice, skipping this cluster.")
# #     return set(persons)
            
            
#     # # Threshold for similarity (0-100)
#     # SIMILARITY_THRESHOLD = 85

#     # # Store duplicates
#     # duplicates = []

#     # for i, person in enumerate(persons):
#     #     first = person.compare_name()
#     #     for j in range(i+1, len(persons)):
#     #         similarity = fuzz.token_sort_ratio(first, persons[j].compare_name())
#     #         if similarity >= SIMILARITY_THRESHOLD:
#     #             duplicates.append((person, persons[j], similarity))
#     # print(f"Found {len(duplicates)} potential duplicates with similarity >= {SIMILARITY_THRESHOLD}")
#     # for dup in duplicates:
#     #     print(f"  {dup[0]} <-> {dup[1]} (similarity: {dup[2]})")

    

# class Person(BaseModel):
#     firstname: str = Field(..., description="First name (mandatory)")
#     fathername: Optional[str] = Field(None, description="Father's name (optional, suffixes removed)")
#     patronymic_marker: Optional[str] = Field(None, description="Patronymic marker (optional)")
#     surname: str = Field(..., description="Surname (mandatory)")
#     aliases: list[str] = Field(default_factory=set, description="List of original name variants")

#     class Config:
#         json_encoders = {
#             set: list  # convert any set to list when serializing to JSON
#         }
        
#     def __init__(self, **data):
#         super().__init__(**data)
#         self.aliases = self.aliases
#         self.firstname = self.firstname.strip().title()
#         self.surname = self.surname.strip().title()
#         self.fathername = None if not self.fathername else (self.fathername
#                                                        .strip()
#                                                        .title()
#                                                        .removesuffix('овна')
#                                                        .removesuffix('евна')
#                                                        .removesuffix('ович')
#                                                        .removesuffix('евич'))
#         self.patronymic_marker = self.patronymic_marker if self.patronymic_marker else self.guess_patronymic_marker()
        
            
#     def __eq__(self, other):
#         if not isinstance(other, Person):
#             return False
#         return (self.firstname, self.surname, self.fathername) == (other.firstname, other.surname, other.fathername)
    
#     def __hash__(self):
#         return hash((self.firstname, self.surname, self.fathername))
    
#     def __gt__(self, other):
#         if not isinstance(other, Person):
#             return NotImplemented
#         return (self.surname, self.firstname, self.fathername or '') > (other.surname, other.firstname, other.fathername or '')
    
#     def __lt__(self, other):
#         if not isinstance(other, Person):
#             return NotImplemented
#         return (self.surname, self.firstname, self.fathername or '') < (other.surname, other.firstname, other.fathername or '')
    
#     def __ge__(self, other):
#         if not isinstance(other, Person):
#             return NotImplemented
#         return (self.surname, self.firstname, self.fathername or '') >= (other.surname, other.firstname, other.fathername or '')
    
#     def __le__(self, other):
#         if not isinstance(other, Person):
#             return NotImplemented
#         return (self.surname, self.firstname, self.fathername or '') <= (other.surname, other.firstname, other.fathername or '')
    
#     def guess_patronymic_marker(self):
#         if self.surname.endswith(('а', 'ә')):
#             return 'кызы'
#         elif self.fathername and self.fathername.endswith(('а', 'ә')):
#             return 'кызы'
#         else:
#             return 'улы'
        
#     def __repr__(self):
#         return self.fullname()
    
#     def name_surname(self):
#         return f"{self.firstname} {self.surname}"
    
#     def name_surname_fathername(self):
#         return " ".join(filter(None, [self.firstname, self.fathername, self.patronymic_marker, self.surname]))
    
#     def fullname(self):
#         if self.fathername:
#             return self.name_surname_fathername()
#         else:
#             return self.name_surname()
        
#     def compare_name(self):
#         return f"{self.firstname} {self.fathername if self.fathername else ''} {self.surname}".strip()
    
#     def consume(self, other: 'Person'):
#         """Merge another Person into this one, combining aliases and filling missing fields."""
#         if not isinstance(other, Person):
#             raise ValueError("Can only merge with another Person instance")
        
#         self.aliases = list(set(self.aliases).union(set(other.aliases)))
        
#         if not self.fathername and other.fathername:
#             self.fathername = other.fathername
#         if not self.patronymic_marker and other.patronymic_marker:
#             self.patronymic_marker = other.patronymic_marker
#         # Note: We do not change firstname or surname to avoid losing original data
        
        
# def _create_persons(persons_file):
#     matched_persons = set()
#     not_matches = []
#     with open(persons_file, "r") as f:
#         persons = [line.strip() for line in f if line.strip()]
#     for p in persons[:]:
#         if "3." in p:
#             p = p.replace("3.", "З.")
#         if match := pattern_fullname_surname_first.match(p):
#             person = Person(
#                 aliases=[p],
#                 firstname=match.group('name'),
#                 surname=match.group('surname'),
#                 fathername=match.group('father_name')
#             )
#             matched_persons.add(person)
#         elif match := pattern_fullname_surname_last.match(p):
#             person = Person(
#                 aliases=[p],
#                 firstname=match.group('name'),
#                 surname=match.group('surname'),
#                 fathername=match.group('father_name')
#             )
#             matched_persons.add(person)
#         elif match := pattern_tatar_father_marker_first.match(p):
#             person = Person(
#                 aliases=[p],
#                 firstname=match.group('name'),
#                 surname=match.group('surname'),
#                 fathername=match.group('father_name'),
#                 patronymic_marker=match.group('father_marker')
#             )
#             matched_persons.add(person)
#         elif match := pattern_tatar_father_marker_last.match(p):
#             person = Person(
#                 aliases=[p],
#                 firstname=match.group('name'),
#                 surname=match.group('surname'),
#                 fathername=match.group('father_name'),
#                 patronymic_marker=match.group('father_marker')
#             )
#             matched_persons.add(person)
#         elif match := pattern_second_word_is_father_name.match(p):
#             person = Person(
#                 aliases=[p],
#                 firstname=match.group('name'),
#                 surname=match.group('surname'),
#                 fathername=match.group('father_name')
#             )
#             matched_persons.add(person)
#         elif match := pattern_surname_last_with_initials_first.match(p):
#             # print(f"  Match: {p} -> {match.groupdict()}")
#             pass
#         elif match := pattern_surname_first_with_initial_last.match(p):
#             # print(f"  Match: {p} -> {match.groupdict()}")
#             pass
#         else:
#             not_matches.append(p)
#     # for nm in not_matches:
#     #     if len(nm.split()) > 2:
#     #         print(f"  Not matched: {nm}")
#     print(f"Total persons: {len(persons)}, not matched: {len(not_matches)}")
#     print(len(matched_persons))
#     return matched_persons
    
    
    

# def _prepare_raw_materials(persons_file, publishers_file):
#     config = read_config()
#     output_dir = get_in_workdir(Dirs.METADATA)
#     _persons = set()
#     _publishers = set()
#     _doc_types = {}
#     _skipped_doc_types = set()
    
#     with Session() as gsheets_session:
#         for meta_file in track(download(bucket=config['yandex']['cloud']['bucket']['metadata'], download_dir=output_dir), "Downloading all metadata files from s3..."):
#             with zipfile.ZipFile(meta_file, 'r') as zf:
#                 _json_files = [f for f in zf.namelist() if f.endswith('.json')]
#                 if len(_json_files) != 1:
#                     raise ValueError(f"Expected exactly one json file in the zip, found {len(_json_files)} in file {meta_file}: {_json_files}")
                
#                 if not (_content := zf.read(_json_files[0])):
#                     print(f"Metadata is empty {meta_file}, skipping it...")
#                     continue
                
#                 meta = json.loads(_content)
#                 # json_meta = json.dumps(json.loads(_content), ensure_ascii=False, indent=None, separators=(',', ':'))
#                 # print(f"Metadata content of {meta_file}: {json_meta}")
#                 # md5 = os.path.basename(meta_file.removesuffix('-meta.zip'))
#                 # doc = gsheets_session.query(Document.md5.is_(md5))
#                 # if not doc:
#                     # raise ValueError(f"Document with md5={md5} not found in the database, cannot update metadata")
            
            
#             if not (doc_type := meta.get("@type")):
#                 raise ValueError(f"No @type field in {meta_file}")
#             if doc_type in _doc_types:
#                 _doc_types[doc_type] = _doc_types[doc_type] + 1
#             else:
#                 _doc_types[doc_type] = 1
                
#             if doc_type not in ['Book', 'CreativeWork', 'Article', 'ScholarlyArticle', 'HowTo', 'Thesis', 'PublicationIssue']:
#                 _skipped_doc_types.add(doc_type)
#                 if doc_type == 'PublicationIssue':
#                     print(meta)
#                 continue
            
#             if publishers := meta.get("publishers"):
#                 for p in publishers:
#                     if p:
#                         if p['@type'] == 'Organization' and (publisher := p.get('name')):
#                             _publishers.add(_normalize(publisher))
#                         else: raise ValueError(f"Unknown publisher type {p['@type']} in {meta_file}")

#             if authors := meta.get("author"):
#                 for a in authors:
#                     if a:
#                         if a['@type'] == 'Person' and (name := a.get('name')):
#                             _persons.add(_normalize(name))
#                         elif a['@type'] == 'Organization' and (name := a.get('name')):
#                             _publishers.add(_normalize(name))
#                         else: raise ValueError(f"Unknown author type {a['@type']} in {meta_file}")
                        
#             if contributors := meta.get("contributors"):
#                 for c in contributors:
#                     if c:
#                         if c['@type'] == 'Person' and (name := c.get('name')):
#                             _persons.add(_normalize(name))
#                         else: raise ValueError(f"Unknown contributor type {c['@type']} in {meta_file}")
                        
#     print(f"Persons ({len(_persons)})")
#     print(f"Publishers ({len(_publishers)})")
#     print(f"Document types ({len(_doc_types)}): {_doc_types}")
#     print(f"Skipped document types ({len(_skipped_doc_types)}): {_skipped_doc_types}")
    
#     with open(persons_file, "w") as f:
#         f.write("\n".join(sorted(_persons)))
#     with open(publishers_file, "w") as f:
#         f.write("\n".join(sorted(_publishers)))
                        
# def _normalize(name):
#     name = name.strip()
#     name = name.replace('“', '"').replace('«', '"').replace('»', '"')
#     # try to split by common separators
#     # name = " ".join([w.strip() for w in name.split(' .') if w.strip()])
#     return name