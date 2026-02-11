"""Prompt templates and helpers for content extraction."""

from __future__ import annotations

import datetime
import io
import json

from google.genai import types
from google.genai.errors import ClientError

from prompts.shots import load_inline_shots

EXTRACT_CONTENT_PROMPT_PRELUDE = '''
# CONTEXT
pages_from = {_from}
pages_to = {_to}
next_footnote_num = {next_footnote_num}
{headers_hierarchy}

# TASK: STRUCTURED_CONTENT

You are extracting structured content from a specific range of pages in a PDF document. The major language is {major_lang}, but can have some inclusions of other languages. The page range is defined by the `pages_from` and `pages_to` values above (inclusive), and refers to the actual page indices in the PDF (not printed page numbers).

Your task is to return a cleaned and structured version of the selected content, formatted in Markdown + HTML, and wrapped under the "content" key of a JSON object.
'''.strip()

EXTRACT_CONTENT_PROMPT_STATIC_BODY = """
1. Remove all headers, footers, and page numbers.
   - These often appear at the top/bottom and may include titles, author names, dates, or printed page numbers.
   - Do **not** confuse genuine section headings with headers.

2. Preserve and structure the main content
   - Keep paragraphs intact and merge broken lines into full sentences.
   - Insert an empty line between paragraphs for readability.
   - Recognize section titles/headings and format them using Markdown headers.
   - Maintain natural reading order.
   - Do **not** translate, rewrite, or omit any legitimate content.

3. Dehyphenate words across line breaks
   - Join words only if a hyphen occurs at the end of a line and the next line begins with its continuation.  
   Example: `мәдә-\nниәт` → `мәдәният`
   - Do **not** merge regular hyphenated words within a line.

4. Apply Markdown formatting
   - Use:
      - `**bold**`
      - `*italic*`
      - `***bold italic***`
   - Only apply formatting if it is clearly visible in the source.
   - Do **not** guess or apply formatting arbitrarily.


5. Format tables using HTML
   - Use `<table>` to format recognized tables.
   - Preserve clarity and structure.
   - Continue tables across page breaks when needed.
   - If a Table of Contents is detected (list of sections and page numbers), do not process its links, page numbers, or headers individually. Instead, preserve its look as a single block using 
   ```html
   <table class="toc"></table>
   ```

6. Detect and format mathematical, physical, or chemical formulas:
   - If a formula is recognized (inline or display), format using LaTeX:
     - Inline formulas: `$...$`
     - Displayed (block) formulas: `$$...$$`
   - Format subscripts as:
      - Scientific context → LaTeX: $H_2O$
      - Non-scientific/stylistic (e.g. indices) → HTML: <sub>...</sub>

7. Detect and format images:
   - Insert images using:
      ```html
      <figure data-bbox="[y_min, x_min, y_max, x_max]" data-page="10"></figure>
      ```
   - The `data-bbox` attribute should contain the bounding box of the image in the following format: `[y_min, x_min, y_max, x_max]`.
     - These coordinates are normalized values between `0` and `1000`.
     - The top-left corner of the page is the origin `(0, 0)`, where:
       - `y_min`: vertical coordinate of the top edge
       - `x_min`: horizontal coordinate of the left edge
       - `y_max`: vertical coordinate of the bottom edge
       - `x_max`: horizontal coordinate of the right edge
     - For example, `[100, 150, 300, 450]` means the image starts 100 units from the top, 150 units from the left, and extends to 300 units down and 450 units across.
   - If a caption is present, format it inside `<figcaption>`, for example:
      ```html
      <figure data-bbox="[100, 150, 300, 450]" data-page="85"><figcaption>Рәсем 5</figcaption></figure>
     ```
   - The `data-page` attribute is exact index in the full PDF (not visible printed page number).
      - The first page you are analyzing might be page 50 in the full document. If so, that is `data-page="50"`.
      - **Ignore visible page numbers in the book itself**. Always use the sequential PDF document index.
   - ⚠️ If the image is located inside a paragraph (e.g., between lines mid-sentence), do not interrupt the paragraph. ❌ Do not insert the image inline in the middle of the paragraph. Instead:
      - Logically split the paragraph into two parts around the image.
      - Place the <figure> after the full paragraph (i.e., append it).
      - Join the paragraph back into a clean, uninterrupted block of text.
      Example Input (detected image between lines):
      ```markdwon
      Кешеләр меңъеллыклар дәвамында  
      [DETECTED IMAGE]  
      табигать белән гармониядә яшәгәннәр.
      ```
      ✅ Correct Output:
      ```markdown
      Кешеләр меңъеллыклар дәвамында табигать белән гармониядә яшәгәннәр.

      <figure data-bbox="[100,150,300,450]" data-page="12"><figcaption>Рәсем 5</figcaption></figure>
      ```
   - If the image is purely decorative (e.g., background ornament), omit it.

8. Format lists
   - Use Markdown bullets (`-`) or numbers (`1.`, `2.`, etc.).
   - Detect and format multi-level lists correctly, preserving indentation and hierarchy.
   Example:
   ```markdown
   - First level
      - Second level
         1. Numbered list inside
   ```
9. Text inside images
   - If there is textual content inside an image, do not extract it.
   - Only represent the image, not its internal text.

10. Continuations across pages
   - If the first paragraph of the current page is a direct continuation from the previous page (i.e., the sentence or word continues across the page break), merge them into one paragraph **without inserting a line break or blank line**.
   - If a table continues from a previous page, continue it without restarting.
   - Apply the same rule for continued tables or formulas.

11. Page Numbering rules:
   - The input slice come from an arbitrary range of the full PDF document (e.g., pages 50–99).
   - Each page in the input corresponds to its **PDF document index**, starting from the specified number (e.g., first page = 50, second = 51, etc.).
   - Use these PDF indices when referencing pages — especially in `data-page` attributes for images.
   - **Do not rely on or mention the printed page numbers inside the scanned document.** Even if a page shows a visible number like "Page 3", ignore it. Use only the sequential index starting from PDF page ${_from} as described.
   - Always use the PDF document index (e.g., page 50, 51, 52...) for data-page, not any printed number shown on the page.
   - Assume the first page provided corresponds to ${_from}.
   - Use this logic for referencing page numbers in images or figure tags.
   
12. Output format
   - Return a JSON object:
   ```json
   {{
      "content": "..."
   }}
   ```
13. General requirements:
   - Output a clean, continuous version of the document, improving structure and readability.
   - Do not translate, rewrite, or modify the original text.
   - Be careful not to accidentally remove important content.
""".strip()

EXTRACT_CONTENT_PROMPT_FOOTNOTE_PART = """
15. Detect and mark footnotes:
   - Maintain global sequential numbering for footnotes starting from {next_footnote_num}: [^{next_footnote_num}]
   - Detect footnotes whether marked by numbers (e.g., 1), symbols (*, †, etc.), or superscripts (<sup>). Normalize all to numbered [^\\d+] format starting from {next_footnote_num}.
   - When you encounter the footnote text, convert it to a standard Markdown footnote definition on a new line:
      ```markdown
      [^1]: This is the text of the first footnote.
      [^2]: This is the text of the second footnote.
      ```
   - 🧾 If footnote texts appear only at the end of the book, treat that section as a footnote glossary - match each footnote to its marker in order of appearance or by matching content when possible. Apply the same global numbering and format as above.
   - Do **not** treat subscripted or superscripted numbers or symbols inside mathematical, physical, or chemical formulas as footnotes. For example, do not convert $H_2O$ or $x^2$ into footnotes. Footnotes should only be detected when:
      - The marker (number, asterisk, or symbol) appears outside any inline or block LaTeX/math context.
      - The marker is not part of a scientific term, chemical formula, or equation.
   When in doubt, prefer LaTeX formatting for anything inside formulas or expressions.
   - ⚠️ Important: If the footnote text appears in the middle of a paragraph, list, or table (e.g., due to page breaks or layout quirks), insert the footnote text definitions before the paragraph, list, or table and only then complete the splitted paragraph, list, or table naturally, without breaks.
   Example:
   Input:
   ```
   Аның фольклорга[^1], тел һәм әдәбият белеменә,
   әдәби тәнкыйтькә караган фәнни хезмәтләре 
   тюркологиядә Нәкый Исәнбәт[^2] дигән галим исемен какша-
   ---
   <sup>1</sup> Н. Исәнбәтнең шәхси архивы: Н. Исәнбәтнең С. Кудашка язган
   хатыннан. 
   <sup>2</sup> Н. Исәнбәтнең шәхси архивы: Әхмәдуллин А. 
   ---
   мас итә (М. Галиев, Ф. Ганиев, М. Госманов, Ә. Еники,
   М. Мәһдиев, Х Мәхмүтов, Г. Рәхим, С. Сафуанов, Б. Урманче, 
   С. Хәким һ.б.).
   ```
   Output (given last global footnote number was 35):
   ```
   [^36]: Н. Исәнбәтнең шәхси архивы: Н. Исәнбәтнең С. Кудашка язган хатыннан.
   [^37]: Н. Исәнбәтнең шәхси архивы: Әхмәдуллин А.
   
   Аның фольклорга[^36], тел һәм әдәбият белеменә, әдәби тәнкыйтькә караган фәнни хезмәтләре тюркологиядә Нәкый Исәнбәт[^37] дигән галим исемен какшамас итә (М. Галиев, Ф. Ганиев, М. Госманов, Ә. Еники, М. Мәһдиев, Х Мәхмүтов, Г. Рәхим, С. Сафуанов, Б. Урманче, С. Хәким һ.б.).
   ```
""".strip()

EXTRACT_CONTENT_PROMPT_POSSIBLE_TITLE = """
16. Document may contain a main title. If you detect a main document title mark it with a single #. Use ## for top-level sections, ### for subsections, and so on. Always preserve the heading hierarchy based on the document's logical structure.
""".strip()

EXTRACT_CONTENT_PROMPT_NO_TITLE = """
16. Document does not have a title page, so never use a single #. Always preserve the heading hierarchy based on the document's logical structure. Current headers hierarchy you can find in the `headers_hierarchy` value. Continue the structure above consistently in this chunk. Do not restart or re-level headings. If a new chapter begins, continue from the next logical chapter number.
""".strip()

def cook_extraction_prompt(batch_from_page, batch_to_page, next_footnote_num, headers_hierarchy, lang_tag):
   """Build the extraction prompt for a page range and language."""
   if headers_hierarchy:
      headers_hierarchy = "\n".join(headers_hierarchy)
      headers_hierarchy =  f"headers_hierarchy = ```\n{headers_hierarchy}\n```"
   else:
      headers_hierarchy = ''
   prompt = [{"text" : EXTRACT_CONTENT_PROMPT_PRELUDE.format(_to=batch_to_page, _from=batch_from_page, next_footnote_num=next_footnote_num, headers_hierarchy=headers_hierarchy, major_lang="Tatar" if lang_tag == 'tt' else "Crimean Tatar")}]

   prompt.append({"text" : EXTRACT_CONTENT_PROMPT_STATIC_BODY.format(_from=batch_from_page)})
   
   prompt.append({"text" : EXTRACT_CONTENT_PROMPT_FOOTNOTE_PART.format(next_footnote_num=next_footnote_num)})

   if batch_from_page:
      prompt.append({"text" : EXTRACT_CONTENT_PROMPT_NO_TITLE.format(headers_hierarchy=headers_hierarchy)})
   else:
      prompt.append({"text" : EXTRACT_CONTENT_PROMPT_POSSIBLE_TITLE})
      
   if lang_tag == 'tt':
      path_to_shots = load_inline_shots()
      with open(path_to_shots, "r") as f:
         prompt.extend(json.load(f))

   prompt.append({"text" : "Now, extract the content from according to the rules above. Return a JSON object with the extracted content."})
   return prompt

def _get_remote_file_or_upload(client, name, content=None, path=None):
   """Fetch an uploaded Gemini file by name, uploading if missing."""
   file = None
   try:
      file = client.files.get(name=name)
      print(f"File `{name}` found")
      if file and file.expiration_time and file.expiration_time - datetime.datetime.now(datetime.UTC) < datetime.timedelta(minutes=30):
        client.files.delete(name=name)
        file = None
   except ClientError as e:
      if e.code != 403:
            raise e
   
   if not file:
      print(f"Uploading file `{name}` to gemini")
      if path:
         file = path
      elif content:
         file=io.BytesIO(content.encode("utf-8"))
      else:
         raise ValueError("Expected either `path` or `content` provided")
      
      file = client.files.upload(
            file=file,
            config=types.UploadFileConfig(
                  mime_type="text/plain",
                  name=name,
            ),
         )
   return file

__all__ = [
    "EXTRACT_CONTENT_PROMPT_PRELUDE",
    "EXTRACT_CONTENT_PROMPT_STATIC_BODY",
    "EXTRACT_CONTENT_PROMPT_FOOTNOTE_PART",
    "EXTRACT_CONTENT_PROMPT_POSSIBLE_TITLE",
    "EXTRACT_CONTENT_PROMPT_NO_TITLE",
    "cook_extraction_prompt",
    "_get_remote_file_or_upload",
]
