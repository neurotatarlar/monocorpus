from string import Template
from google.genai.errors import ClientError
from google.genai import types
import io
import os
import datetime
from prepare_shots import load_inline_shots
import json

cooked_shots_dir = "./shots/cooked"

# SYSTEM_PROMPT="""
# You are an expert assistant specializing in processing Tatar-language documents written in Cyrillic script.
# You perform two types of tasks based on the user’s instruction:

# 1. Structured Content Extraction:
# - Extract and format the document’s main content into Markdown with some embedded HTML.
# - Focus on preserving the structure: paragraphs, headings, tables, formulas, images, footnotes, subscripts, and tables of contents.
# - Do not translate, summarize, or rewrite the original text.
# - Maintain natural reading order and document flow.
# - Handle content that spans pages (e.g., continuing paragraphs, tables).
# - Maintain the natural reading order throughout the document.

# 2. Metadata Extraction:
# - Extract metadata (such as title, author, language, publisher, publication date) by analyzing the first few and last few pages of the document.
# - Return metadata structured in Schema.org format as valid JSON-LD.
# - When information is missing, leave fields blank or omit them.
# - Do not invent information that does not exist in the document.
# - Language of metadata values must remain in original Tatar language unless otherwise specified.

# In both tasks:
# - Be precise, structured, and careful.
# - Prioritize accuracy and document integrity over speculation or creativity.
# - Never translate, summarize, or paraphrase unless explicitly requested.
# """

EXTRACT_CONTENT_PROMPT_PRELUDE = """
# TASK: STRUCTURED_CONTENT

You are extracting structured content from a Tatar-language slice of pages from a document (pages {_from}-{_to} inclusive). Please process the content according to the following instructions and return the result as a JSON with the document's structured content in the 'content' property, formatted using Markdown and HTML.
""".strip()

EXTRACT_CONTENT_PROMPT_STATIC_BODY = """
1. Remove all headers, footers, and page numbers.
   - These often appear at the top or bottom of each page and may include titles, chapter names, author names, page numbers, or dates.
   - Do not confuse page headers with genuine section titles appearing at the start of a page.

2. Preserve and identify structural elements:
   - Keep paragraphs intact.
   - Recognize and properly format section titles or headings.
   - Do not modify, translate, or rewrite the original text.
   - Insert empty lines between paragraphs for readability.
   - Merge lines within the same paragraph, title, or header.
   - If a word is hyphenated across lines (e.g., "мәдә-\nниәт"), join it correctly ("мәдәният"). Only join the word if the break occurs at the end of a line and the next line begins with the continuation of the same word. Do not join words separated by hyphens in the middle of a sentence unless it's clearly a line break artifact.
   - Maintain the natural reading order throughout the document.

3. Preserve text formatting using Markdown syntax:
   - Bold text should be wrapped in double asterisks: **bold**.
   - Italic text should be wrapped in single asterisks: *italic*.
   - Bold italic text should use triple asterisks: ***bold italic***.
   - Preserve inline styles exactly as they appear in the original (e.g., bold names, italicized quotes or terms).
   - Do not guess or apply formatting arbitrarily—only use bold/italic when it is clearly visually marked.

4. Detect and format tables:
   - Format detected tables using HTML `<table>`.
   - Ensure the structure remains readable and clear.
   - If a table continues from a previous page, continue it without restarting.
   - If the detected text appears to be a Table of Contents(list of sections/chapters with page numbers), do not process its links, page numbers, or headers individually. Instead, preserve its look as a single block using <table class="toc"></table>.

5. Detect and format mathematical, physical, or chemical formulas:
   - If a formula is recognized (inline or display), format using LaTeX:
     - Inline formulas: `$...$`
     - Displayed (block) formulas: `$$...$$`

6. Detect and format subscripts:
   - If subscripted text appears:
     - If it is part of a scientific, mathematical, physical, or chemical formula, format it using LaTeX syntax (e.g., `$H_2O$`).
     - Otherwise, if stylistic (e.g., chapter number, index), format it using HTML `<sub>...</sub>`.
   - When unsure, prefer LaTeX if the context appears scientific/mathematical.

7. Detect and format images:
   - Insert images using:
     ```html
     <figure data-bbox="[y_min, x_min, y_max, x_max]" data-page="10"></figure>
     ```
   - The `data-page` attribute indicates the page number the image was found on (starting from 1).
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

8. Preserve lists:
   - Use Markdown bullets (`-`) or numbers (`1.`, `2.`, etc.).
   - Detect and format multi-level lists correctly, preserving indentation and hierarchy.
   Example:
   ```markdown
   - First level
      - Second level
         1. Numbered list inside
   ```
9. Images and embedded text:
   - If there is textual content inside an image, do not extract it.
   - Only represent the image, not its internal text.

10. Handling content continuation across pages:
   - If the first paragraph of the current page is a direct continuation from the previous page (i.e., the sentence or word continues across the page break), merge them into one paragraph **without inserting a line break or blank line**.
   - If a table continues from a previous page, continue it without restarting.

11. General requirements:
   - Output a clean, continuous version of the document, improving structure and readability.
   - Do not translate, rewrite, or modify the original Tatar text.
   - The document language is Tatar, written in Cyrillic.
   - Be careful not to accidentally remove important content.
""".strip()

EXTRACT_CONTENT_PROMPT_FOOTNOTE_PART = """
12. Detect and mark footnotes:
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
   - ⚠️ Important: If the footnote text appears in the middle of a paragraph, list, or table (e.g., due to page breaks or layout quirks), do not insert it immediately. Instead, complete the full paragraph, list, or table naturally, without breaks, then insert the footnote text definitions after it.
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
   Аның фольклорга[^36], тел һәм әдәбият белеменә, әдәби тәнкыйтькә караган фәнни хезмәтләре тюркологиядә Нәкый Исәнбәт[^37] дигән галим исемен какшамас итә (М. Галиев, Ф. Ганиев, М. Госманов, Ә. Еники, М. Мәһдиев, Х Мәхмүтов, Г. Рәхим, С. Сафуанов, Б. Урманче, С. Хәким һ.б.).
   
   [^36]: Н. Исәнбәтнең шәхси архивы: Н. Исәнбәтнең С. Кудашка язган хатыннан.
   [^37]: Н. Исәнбәтнең шәхси архивы: Әхмәдуллин А.
   ```
""".strip()

EXTRACT_CONTENT_PROMPT_POSSIBLE_TITLE = """
13. Document may contain a main title. If you detect a main document title mark it with a single #. Use ## for top-level sections, ### for subsections, and so on. Always preserve the heading hierarchy based on the document's logical structure.
""".strip()

EXTRACT_CONTENT_PROMPT_NO_TITLE = """
13. Document does not have a title page, so use ## for the highest-level headings, ### for subsections, and so on. Never use a single #. Always preserve the heading hierarchy based on the document's logical structure.
""".strip()

EXTRACT_CONTENT_PROMPT_PREV_CHUNK_TAIL = """
14. The last slice of content from the previous chunk is provided below. Use this reference to continue any **broken paragraphs, sentences, lists, tables, or other structures** that begin in the current chunk. 
   - When the current chunk starts mid-sentence or mid-structure, join it **seamlessly and naturally** to the reference content, without duplicating or breaking the flow.
   - Do **not** insert extra blank lines or headings when continuing content.
   - Apply **all the same processing rules** (headers/footers removal, image formatting, dehyphenation, etc.) to the joined result.
   - ⚠️ If no continuation is necessary, proceed with the current chunk as a standalone unit.
   📎 Previous chunk tail (for reference only, do not repeat):
"""

DEFINE_META_PROMPT=Template("""
# TASK: METADATA_EXTRACTION

You are given a PDF document that contains the first ${n} and last ${n} pages of a book.

Text may appear in different scripts:
- Tatar in Cyrillic script → use `"tt-Cyrl"`
- Tatar in Zamanalif Latin script → use `"tt-Latn-x-zamanalif"`
- Tatar in Yanalif Latin script → use `"tt-Latn-x-yanalif"`
- Tatar in Arabic script → use `"tt-Arab"`
- Russian in Cyrillic script → use `"ru-Cyrl"`

Automatically detect the **primary language and script** used in the document, and return the correct `inLanguage` BCP 47 tag.

Extract metadata in Schema.org Book format using compact JSON-LD.

Follow these rules strictly:
- **Use UTF-8 characters**.
- **Only extract** data that is **explicitly present** in the text.  
  **Do not guess, assume, or invent** any values.
- **Dehyphenate** words broken across lines in the extracted output.
- **Omit** any property that is not found — **do not** include nulls, placeholders, or default values.

Extract metadata such as:
- `name` — Title of the book
- `author` — Author(s)
- `contributor` — Persons involved in roles such as: author, editor, translator, illustrator, composer, lyricist, contributor, reviewer, publisher, sponsor. Express roles in English
- `publisher`
- `datePublished` — Year of publication
- `isbn`
- `inLanguage` — As detected, using correct BCP 47 tag
- `description` — Preface, abstract, or annotation in the books's primary language
- `numberOfPages`
- `bookEdition`
- `additionalProperty` — Use this to include UDC, BBK, or other classification codes
- `genre` — Book genre, expressed in English
- `audience` — Target audience, expressed in English

Markdown formatted Examples of input:
- Tatar Cyrillic: # ТАТАРСТАН РЕСПУБЛИКАСЫ КОНСТИТУЦИЯСЕ\n(2002 елның 19 апрелендәге 1380 номерлы, 2003 елның 15 сентябрендәге 34-ТРЗ номерлы, 2004 елның 12 мартындагы 10-ТРЗ номерлы, 2005 елның 14 мартындагы 55-ТРЗ номерлы, 2010 елның 30 мартындагы 10-ТРЗ номерлы, 2010 елның 22 ноябрендәге 79-ТРЗ номерлы, 2012 елның 22 июнендәге 40-ТРЗ номерлы Татарстан Республикасы законнары редакциясендә)\n\nӘлеге Конституция, Татарстан Республикасының күпмилләтле халкы һәм татар халкы ихтыярын чагылдырып, \nкеше һәм граждан хокукларының һәм ирекләренең өстенлеген гамәлгә ашыра, халыкларның гомумтанылган үзбилгеләнү хокукына, аларның тигез хокуклылыгы, ихтыяр белдерүнең иреклелеге һәм бәйсезлеге принципларына нигезләнә,\nтарихи, милли һәм рухи традицияләрнең, мәдәниятләрнең, телләрнең сакланып калуына һәм үсешенә, гражданнар татулыгын һәм милләтара килешүне тәэмин итүгә ярдәм итә, \nфедерализм принципларында демократиянең ныгуы, Татарстан Республикасының социаль-икътисадый үсеше, Россия Федерациясе халыкларының тарихи барлыкка килгән бердәмлеген саклап калу өчен шартлар тудыра.\n\n## I КИСӘК. КОНСТИТУЦИЯЧЕЛ КОРЫЛЫШ НИГЕЗЛӘРЕ\n### 1 статья\n1. Татарстан Республикасы – Россия Федерациясе Конституциясе, Татарстан Республикасы Конституциясе һәм «Россия Федерациясе дәүләт хакимияте органнары һәм Татарстан Республикасы дәүләт хакимияте органнары арасында эшләр бүлешү һәм үзара вәкаләтләр алмашу турында» Россия Федерациясе һәм Татарстан Республикасы Шартнамәсе нигезендә Россия Федерациясе белән берләшкән һәм Россия Федерациясе субъекты булган демократик хокукый дәүләт. Татарстан Республикасы суверенитеты, Россия Федерациясе карамагындагы мәсьәләләрдән һәм Россия Федерациясе һәм Татарстан Республикасының уртак карамагындагы мәсьәләләр буенча Россия Федерациясе вәкаләтләреннән тыш, дәүләт хакимиятенең (закон чыгару, башкарма һәм суд) бөтен тулылыгына ия булуда чагыла һәм Татарстан Республикасының аерылгысыз хасияте була.\n\n2. Татарстан Республикасы һәм Татарстан исемнәре бер үк мәгънәгә ия.\n\n3. Татарстан Республикасы статусы Татарстан Республикасының һәм Россия Федерациясенең үзара ризалыгыннан башка үзгәртелә алмый. Татарстан Республикасы чикләре аның ризалыгыннан башка үзгәртелә алмый. 
- Tatar Latin(Zamanalif): Tatarstan Respublikası Ministrlar Kabinetı üz ormativ-xoquqıy aktların älege Zakonğa yaraqlaştırırğa tieş.\n\nTatarstan Respublikası Prezidentı **M. Şäymiev**.\n\nQazan şähäre, 1999 yıl, 15 sentäbr. №2352.\n\n## Alfavit häm orfografiä\nOrfografiä — döres yazu qağidäläre digän süz. Ul bilgele ber alfavitqa nigezlänä. Bu orfografiä Tatarstan Respublikası Prezidentı tarafınnan 1999 yılnıñ 15 sentäbrendä qul quyılğan Zakonda qabul itelgän alfavitqa nigezlänep tözelde.\n\nYaña alfavit 34 xäreftän tora, anda suzıq awazlarnı belderüçe — 9, tartıqlarnı belderüçe — 25 xäref kürsätelgän. Apostrof, siräk qullanılğanlıqtan, alfavitta ayırım urın almağan, ul, hämzäne (tä’min) belderüçe häm neçkälek bilgese bularaq, barı orfografiädä genä isäpkä alına.\n\nBu zakondağı alfavit, nigezdä, 1927—1939 yıllarda qullanılğan “Yañalif” alfavitın yañadan torğızuğa qaytıp qala. Läkin biredä “Yañalif”ne tulısınça şul kileş kire qaytaru yuq, häm ul bula da almıy, çönki anıñ qullanılmawına 60 yıl ütte, tormış üzgärde: yazuları latin grafikasına nigezlängän Könbatış tellären öyränü massaküläm küreneşkä äylände, xalıqara urtaq kompyuterlar belän eş itü, xätta dönyaküläm informatsiä sistemasına — internetqa çığu ğädätkä kerde, törki xalıqlarnıñ üzara aralaşa, ber-bersen ruxi bayıta alu mömkinlekläre açıldı.\n\nMenä şul şartlarda “Yañalif” üzgärtelmiçä torğızılğan bulsa, tatar balası, tatar häm çit il latinitsaları arasındağı ayırmalarnı kübräk kürep, qıyın xäldä yışraq qalır ide, tatar keşese, kompyuter qullanğanda, bigräk tä anıñ yärdämendä internetqa çığıp eşlägändä, qıyınlıqlarnı kübräk kürer ide, törki tuğannarınıñ yazuların uqırğa turı kilsä dä, törle çitenleklärgä duçar bulır ide.\n\nŞuşı äytelgännärne istä totıp, TR Däwlät Sovetı Zakonğa “Yañalif”ne beraz üzgärtep tözelgän yaña alfavitnı täqdim itte, häm ul, bilgele, kimçelekläre bulsa da, xäzerge zaman taläplärenä nığraq cawap birä.\n\n## Tatar orfografiäsen tözü prinsipları\nHärber telneñ orfografiäse törle prinsiplarğa nigezlänep tözelä. Tatar orfografiäse tübändäge prinsiplarğa nigezlängän.\n\n**Fonetik prinsip** — işetelgänçä yazu digän süz. Tatarnıñ töp süzläre häm tatarça äyteleşkä buysınğan yäki turı kilgän alınmalar işetelgänçä, yäğni fonetik prinsipqa nigezlänep yazılalar: äni, ulım, öydägelär, kürşe awıllarda, büränä, säläm, kitap, namaz, magazin h.b.\n\n**Grafik prinsip** — alınma süzlärne birüçe teldägegä oxşatıp yazu digän süz. Tatarça äyteleşkä buysınıp citmägän alınma süzlär, grafik prinsipqa nigezlänep, birgän teldäge yazılışqa oxşatıp yazılalar: tarixi (tarixıy tügel), Talip (Talıyp tügel); morfologiä (marfologiä tügel), motor (mator tügel), traktor (traktır tügel) h.b.\n\n***İskärmä.*** Tarixi, Talip kebek süzlärdä i yazu (ıy yazmaw) misalında bez ekonomiä prinsibın da küzätäbez [Ekonomiä prinsibınıñ 3-nçe punktın qarağız].\n\n**Morfologik prinsip** — söylämdä üzgäreşkä oçrağan morfemanı yazuda üzgäreşsez qaldıru: [umber, umbiş] digändä un morfeması [b] awazı tä’sirendä üzgärä, läkin ul üzgäreş yazuda kürsätelmi, un morfeması saqlana: un ber, un biş, yaz — yazsa (yassa tügel), süzçän (süsçän tügel), rusça (ruçça tügel), irtänge (irtäñge tügel), isänme (isämme tügel) h.b.\n\n**Ekonomiyä prinsibı** — yazu protsessında waqıtqa häm urınğa ekonomiä yasaw öçen, süzlärne qısqartıp yazu digän süz. Bu prinsip şaqtıy küp küzätelä.\n\n1. Teldä yış qullanıla häm küplärgä tanış quşma atamalar andağı süzlärneñ baş xäreflären genä yazu yulı belän qısqartılalar: Berläşkän millätlär oyışması — BMO; Tatarstan Respublikası Ministrlar Kabinetı — TR MK; Tatarstan Fännär akademiäse — TFA; Tel, ädäbiyat häm sänğät institutı — TÄhSİ.\n\nYış oçrıy torğan ike süz qısqartılğanda, ul süzlärneñ berençe (yul) xärefläre genä noqta quyılıp yazıla: häm başqalar — h.b.; häm başqa şundıylar — h.b.ş.\n\nKüplärgä tanış bulmağan atamalarnı qısqartıp yazarğa kiräk bulğanda, ayırım tekstlarda ul atama başta tulısınça yazıla, şunda uq cäyälär eçendä anıñ qısqartılması birelä, ul tekstta annan soñ barı qısqartılma süz genä yazıla, mäsälän, Min bu mäqälämdä Tatarstan Respublikasınıñ Ekologiä institutı (TR Eİ) turında söylärgä cıyınam,— dip kürsätkännän soñ, avtor bu süzlär tezmäsen yañadan tulısınça yazmıy, anı barı TR Eİ dip kenä qısqartıp birä.\n\n2. Quşma atamalardağı yä ber süzneñ, yä barlıq süzlärneñ dä yä ike xärefe, yä ber icege yazıla: KamAZ, AlAZ, YuXİDİ, KamGes, univermag h.b. Quşma süzneñ soñğısı tulı kileş, baştağıları qısqartılıp yazılırğa da mömkin: dramtügäräk, Tatpotrebsoyuz, Kazjilstroy h.b.\n\n3. Ekonomiä prinsibı yarımäyteleşle awazlarnı yazuda kürsätmäwdä dä çağıla, mäsälän, su süzendä ike awaz arasında ı işetelgän kebek bula, läkin ul yazuda kürsätelmi (sıu dip yazılmıy); uqı, tuqı süzlärenä [u] awazı quşılğaç, [u] aldınnan [ı] işetelgän kebek bula, läkin ul yazuda kürsätelmi, uqıu dip yazılmıy, uqu dip yazıla; baru, kilü kebek fiğellärdä, tartım quşımçası aldınnan [w] işetelğän kebek bula [baruwı, kilüwe], läkin ul yazuda çağılmıy, baruı, kilüe räweşendä genä yazıla; iä, iäk, orfografiä kebek süzlärdä, [i] häm [ä] awazları arasında [y] işetelgän kebek bulsa da, anı, ekonomiä prinsibınnan çığıp, yazuda kürsätmilär. (Tağın 31-nçe §nıñ 2-nçe iskärmäsen häm 33-nçe §nıñ 2-nçe iskärmäsen qarağiz).\n\n**Tarixi prinsip** — başqaçaraq işetelsä dä, süzlärne elekke çordağıça yazu digän süz. Bu prinsip iske yazulı tellärdä (mäs., ingliz telendä) yış küzätelä, tatar telendä yuq däräcäsendä az. [o], [ö] awazları berençe icektä genä tügel, ikençe, öçençelärendä äytelsä dä, alarnı barı berençe icektä genä yazarğa digän qağidä elektän “Yañalif” orfografiäsennän küçerelde, dimäk, anıñ yazılışı, bilgele däräcädä, tarixi prinsipqa nigezlängän.\n\n## Döres yazu qağidäläre\n\n### Suzıq awaz xärefläreneñ yazılışı\n\n§ 1. A xärefe [a] awazı äytelgän här urında yazıla: ağaç, qara, kamzul, garmun h.b. 
- Tatar Latin(Yanalif): Quzƣal, ujan, ləƣnət itelgən\nQollar həm aclar dɵnjasь,\nDoşmannan yc alsьn tygelgən\nYksezlər, tollar kyz jəşe\nQanlь suƣьşqa ʙez çьƣarʙьz,\nÇimererʙez iske dɵnjanь!\nAnьꞑ urьnьna ʙez qorьrʙьz,\nTьzerʙez matur, jaꞑanь!\n\nBu ʙulьr iꞑ axьrƣь, iꞑ qatь zur çihat,\nBulьr həm ʙəjlnlmilər ʙəni insan azat!\n\nBezne hic kem azat itə almas,\nItsək — itərʙez yzeʙez,\nBezne hic kem şat itə almas,\nItsək — itərʙez yzeʙez,\nƏjdnə zalimnərgə ʙez qarşь\nƢəjrət ʙelən suƣьşьp ʙarьjq,\nTusьn ʙalqьp irek qojaşь,\nXoquqlarьʙьnь alьjq!\n\nBu ʙulьr iꞑ axьrƣь, iꞑ qatь zur çihat,\nBulьr həm ʙəjlnlmilər ʙəni insan azat!\n\nBez ʙar çihan eşceləreʙez,\nBez ʙar dɵnjanьꞑ ƣəskər,\nÇirlər ʙezneꞑ yz çirləreʙez,\nBeznęder ʙar dəylətləre!\nCьƣьjq ʙez məjdanьna ʙez,\nDoşmannar xur ʙelən qacar,\nCьƣaʙьz həm inanaʙьz\nQojaş ʙezgə nurьn cəcər!\n\nBu ʙulьr iꞑ axьrƣь, iꞑ qatь zur çihat,\nBulьr həm ʙəjlnlmilər ʙəni insan azat! Barlıq keşelər də azat həm üz abruyları həm xoquqları yağınnan tiꞑ bulıp tualar. Alarğa aqıl həm wɵcdan birelgən həm ber-bersenə qarata tuğannarça mɵnasəbəttə bulırğa tieşlər.  
- Tatar Arabic: کتاب\n،هیچده کوڭلم آچلماسلق اچم پوشسه\n،اوز اوزمنی کوره‌لمیچه روحم توشسه\nجفا چیکسه‌م، جوده‌ب بتسه‌م بو باشمنی\n،قویالمیچه جانغه جلی هیچ بر توشکه\n،حسرت صوڭره حسرت کیلب آلماش، آلماش\n،کوڭلسز اوی بله‌ن تمام ئه‌یله‌نسه باش\nکوزلرمده کیببده جیتمگان بولسه\n.حاضرگنه صغلوب، صغلوب جلاغان یاش\nشول وقتده مین قولیمه کتاب آلام\nآنڭ ایزگی صحیفه‌لرن آقتارام\n.راحتله‌نوب کیته شونده جانم، ته‌نم\n.شوندنغنه دردلريمه درمان طابام\n،اوقوب بارغان هربر یولم، هربر سوزم\nبولا مینم یول کورسه‌تکوچی یولدزم\nسویمی باشلیم بو دنیانڭ واقلقلرن\n.آچیلا‌در، نورلانا‌در کوڭلم، کوزم\nجیڭلله‌نه‌م، معصومله‌نه‌م مین شول چاقده\nرحمت ئه‌یته‌م اوقوغانم شول کتابقه\n،اشانچم آرطه‌ مینم اوز اوزیمه\n.امید برلن قاری باشلیم بولاچقغه\nاوز اوزیمه\n،تلیم بولورغه مین انسان علی\n.تلی کوڭلم تعالی بالتوالی\nکوڭلم برلن سویه‌م بختن تاتارنڭ\nکوررگه جانلیلق وقتن تاتارنڭ\n.تاتار بختی اوچون مین جان آتارمن\n.تاتار بیت مین اوزمده چن تاتارمن\n،حسابسز کوب مینم ملتکه وعده م\n.قرلماسمی واوی، والله اعلم\n

ERROR HANDLING:

1. If no metadata can be extracted with certainty:
   - Output an empty JSON object: {}

2. If multiple possible values exist (e.g., several titles):
   - Prefer the first clearly indicated value.
   - Do not combine or merge multiple options into one field.

3. If year of publication is given ambiguously (e.g., "circa 1980s," "not earlier than 1995"):
   - Omit `datePublished`.

5. If a field value is partially damaged or incomplete:
   - Omit the field rather than risking incorrect data.

REMINDERS:
- ❌ Never guess or hallucinate any information.
- ❌ Never fabricate missing fields.
- ✅ Always prioritize accuracy and certainty.

📌 Output only the final clean JSON-LD object.  
📌 No explanations, no Markdown, no comments — only raw JSON-LD.
""")

def cook_extraction_prompt(batch_from_page, batch_to_page, next_footnote_num, prev_chunk_tail, client):
   prompt = [{"text" : EXTRACT_CONTENT_PROMPT_PRELUDE.format(_to=batch_to_page+1, _from=batch_from_page+1)}]

   prompt.append({"text" : EXTRACT_CONTENT_PROMPT_STATIC_BODY})
   
   prompt.append({"text" : EXTRACT_CONTENT_PROMPT_FOOTNOTE_PART.format(next_footnote_num=next_footnote_num)})

   if batch_from_page:
      prompt.append({"text" : EXTRACT_CONTENT_PROMPT_NO_TITLE})
   else:
      prompt.append({"text" : EXTRACT_CONTENT_PROMPT_POSSIBLE_TITLE})
      
   if prev_chunk_tail:
      prompt.append({"text" : EXTRACT_CONTENT_PROMPT_PREV_CHUNK_TAIL})
      prompt.append({"text" : prev_chunk_tail})
      
   path_to_shots = load_inline_shots()
   with open(path_to_shots, "r") as f:
      prompt.extend(json.load(f))

   prompt.append({"text" : "Now, extract structured content from the following document"})
   return prompt


def _get_remote_file_or_upload(client, name, content=None, path=None):
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