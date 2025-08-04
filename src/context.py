import os
from utils import workdir

context_file_name = "GEMINI.md"

EXTRACT_CONTENT_CONTEXT = r"""
# TASK: STRUCTURED_CONTENT

You are extracting structured content from a specific range of pages in a PDF document written in the Tatar language. The page range is defined by the `pages_from` and `pages_to`(inclusive) values in the prompt's context, and refers to the actual page indices in the PDF (not printed page numbers).

Your task is to return a cleaned and structured version of the selected content, formatted in Markdown + HTML, and wrapped under the "content" key of a JSON object.

## Guidelines for Content Extraction
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
      <figure data-bbox="[100, 150, 300, 450]"><figcaption>Рәсем 5</figcaption></figure>
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
   - **Do not rely on or mention the printed page numbers inside the scanned document.** Even if a page shows a visible number like "Page 3", ignore it. Use only the sequential index starting from PDF page `pages_from` value in prompt as described.
   - Always use the PDF document index (e.g., page 50, 51, 52...) for data-page, not any printed number shown on the page.
   - Assume the first page provided corresponds to `pages_from` value in prompt.
   - Use this logic for referencing page numbers in images or figure tags.
   
12. Language
   - The document is written in Tatar using mistly in the Cyrillic script.
   - Be cautious not to delete or alter meaningful Tatar-language content.

13. Output format
   - Return a JSON object:
   ```json
   {{
      "content": "..."
   }}
   ```
14. General requirements:
   - Output a clean, continuous version of the document, improving structure and readability.
   - Do not translate, rewrite, or modify the original Tatar text.
   - The document language is Tatar, written in Cyrillic.
   - Be careful not to accidentally remove important content.

15. Detect and mark footnotes:
   - Maintain global sequential numbering for footnotes starting from `next_footnote_num` value in prompt.
   - Detect footnotes whether marked by numbers (e.g., 1), symbols (*, †, etc.), or superscripts (<sup>). Normalize all to numbered [^\\d+] format starting from `next_footnote_num` value in prompt.
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
   
## Examples of expected output

Please review these examples and follow their formatting when encountering similar cases

### Example 1: Single paragraph.

**Description (Textual):**
- Text is center-aligned on the page.
- Appears as a single block of prose (not verse or table).
- Uses large serif font.
- No headings, numbering, or formatting.
- Text expresses a formal declaration (often legal or constitutional).
- Line breaks are not meaningful; the text should be treated as a single paragraph.

**Extraction Instructions:**
- Join all lines into one continuous paragraph.
- Preserve punctuation (commas, dashes, periods).
- Do not add any extra formatting (like headings, bullets, or titles).
- Output as a single Markdown paragraph.

**Expected json with Markdown content:**
```json
{
  "content": "Кеше, аның хокуклары һәм ирекләре – иң зур хәзинә. Кеше һәм граждан хокукларын һәм ирекләрен тану, үтәү һәм яклау – Татарстан Республикасының бурычы."
}
```

### Example 2: Section with Heading, Subheading, and Highlighted Definition

**Description (Textual):**  
- The page contains **structured educational content** from a textbook.
- It starts with a **main topic title** in bold (e.g., chapter name).
- Below it is a **section number and title**: bold and prefixed with a paragraph (§) symbol.
- One or more introductory paragraphs follow in standard font.
- A key definition is presented inside a **highlighted box** (blue background), with the term **bolded and italicized**.
- The rest of the page is typical paragraph text, including examples and formula-like structures.

**Extraction Instructions:**
- Extract the **main title** as a level 2 Markdown heading (`##`).
- Extract **section headings** as level 3 Markdown headings (`###`), keeping the `§` symbol and number.
- Extract regular prose paragraphs with line breaks between them.
- If there is a **highlighted definition**, extract it as a blockquote using `>` and preserve bold/italic formatting.
- Use LATEX formatting for atomic symbols, if available (e.g., `$^{35}_{17}\text{Cl}$`). 
**Expected json with Markdown content:**
```json
{
  "content": "## Иң әһәмиятле химик төшенчәләр һәм законнар. I БҮЛЕК\n\n### § 1. Химик элемент. Изотоплар\n\nХимиядә иң әһәмиятле төшенчәләрнең берсе — химик элемент.\n\n***Химик элемент*** ул — химик яктан бер-берсе белән бәйләнмәгән, төш корылмалары бердәй булган атомнар төре.\n\nБу билгеләмәдән күренгәнчә, химик элемент — ниндидер бер атом түгел, бәлки төш корылмасы (протоннар саны) бердәй булган атомнар җыелмасы. Бер үк элемент атомнары төшләрендәге нейтроннар саны аерылырга мөмкин. Мәсәлән, хлор элементы, төшендәге нейтроннар саны белән аерылып торучы ике төрле атомнардан — $^{37}_{17}\text{Cl}$ атомнарыннан һәм $^{35}_{17}\text{Cl}$ атомнарыннан тора. Бу атомнарны хлорның изотоплары дип атыйлар."
}
```

### Example 3: Section Title and Paragraph (No Boxed Definitions)

**Description (Textual):**  
- The page contains a **section heading** starting with a paragraph (§) symbol and a number.
- The section heading is **bold and large**, typical of textbook structure.
- The text below is a **single paragraph of prose**, no bullet points or formatting boxes.
- Font is standard serif; alignment is left-justified.
- Text includes italicized Latin letters (e.g., *s-* and *p-* elements), but no images or chemical formulas.

**Extraction Instructions:**
- Extract the **section heading** as a level 2 Markdown heading (`##`), including the `§` symbol and number.
- The paragraph that follows should be output as a single Markdown paragraph with preserved punctuation.
- Preserve **italic formatting** for Latin characters (e.g., *s-* and *p-*).
- Do not insert artificial breaks or add extra formatting.

**Expected json with Markdown content:**
```markdown
{
   "content": "## §21. А-төркемнәрнең металлик элементларына күзәтү\n\nМеталлик s- һәм р- элементларның гомуми химик үзлеге булып аларның атом радиусы неметаллик элементларның атом радиусы белән чагыштырганда шактый зуррак булуы аркасында, валентлык электроннарын җиңел бирә алулары тора. Шуңа күрә алар барлыкка китергән гади матдәләр химик реакцияләрдә кайтаргычлар функциясен үтиләр."
}

```
"""

def prepare():
   context_dir = os.path.join(os.path.expanduser(workdir), "context")
   context_file = os.path.join(context_dir, context_file_name)
   if not os.path.exists(context_file):
      print(f"Preparing context file at {os.path.abspath(context_file)}")
      os.makedirs(context_dir, exist_ok=True)
      # full_context = [EXTRACT_CONTENT_CONTEXT]

      # full_context.append("# TASK: STRUCTURED_CONTENT\n\n" + EXTRACT_CONTENT_CONTEXT.strip())
      # # copy example images and ground truth to context directory
      # _dir = ("./shots/snippets")
      # gt = _list_files(_dir, endswith='.md')
      # context_files = set()
      # for ground_truth_path in gt:
      #    _id, _ = os.path.splitext(os.path.basename(ground_truth_path))
      #    _id = _id[:-1]
      #    image_path = os.path.join(_dir, f"{_id}1.jpeg")
         
      #    # copy image and ground truth to context in the working directory
      #    shutil.copy(image_path, context_dir)
      #    shutil.copy(ground_truth_path, context_dir)
         
      #    image_file_name = os.path.basename(image_path)
      #    ground_truth_file_name = os.path.basename(ground_truth_path)
      #    full_context.append(f"- Image JPEG: @{image_file_name} → Ground Truth: @{ground_truth_file_name}")
      #    context_files.update([image_file_name, ground_truth_file_name])
         
      # full_context.append("Each `.jpeg` file shows the original scanned page area. The `.md` file shows the expected Markdown output.")
      with open(context_file, "w") as f:
         f.write(EXTRACT_CONTENT_CONTEXT)
         
      # update settings.json to point to the context files
      # settings_file = get_in_workdir(".gemini", file="settings.json")
      # with open(settings_file, "r") as f:
      #    settings = json.load(f)
      #    current_context_files = set(settings.get("contextFileName", context_file_name))
      #    current_context_files.update(context_files)
      #    settings["contextFileName"] = list(current_context_files)
      # with open(settings_file, "w") as f:
      #    json.dump(settings, f, indent=4, ensure_ascii=False)


def _list_files(dir, endswith):
    return [os.path.join(dir, f) for f in os.listdir(dir) if f.endswith(endswith)]
