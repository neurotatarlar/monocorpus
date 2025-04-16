PROMPT = """
You are given a PDF document written in Tatar (Cyrillic script). Please process the content according to the following instructions and return the result as a Markdown + HTML object.

---- Start of instruction -----
1. Remove all headers, footers, and page numbers. These often appear at the top or bottom of each page and may include titles, chapter names, author names, page numbers, or dates.

2. Preserve and identify structural elements:
- Keep paragraphs intact.
- Recognize and preserve section titles or headings.
- Do not modify the original language or content, just clean and format the structure.
- Use empty lines between paragraphs for readability.

3. Detect and format tables:
- If a table is recognized, format it using HTML table.
- Keep the structure readable and clear.

4. Detect and format mathematical, physical, or chemical formulas:
- If a formula is recognized (either inline or displayed), render it using Markdown LaTeX syntax:
  - Inline formulas → $...$
  - Displayed formulas → $$...$$

5. Detect images and insert `<!-- picture -->`

6. Keep natural reading order.

7. If text or table continues on the next page, merge them into a single one.

8. Keep list items as Markdown bullet/number. Be careful, sometimes the list can be multileveled.

9. If there is text belonging to an image, such as a caption or text inside the image, do not extract it. Leave it as part of the image.

10. If the image is a background or an ornament, omit it.

11. Only the main title can be formatted with a single markdown `#`, other headers should use multiple `#` based on their hierarchy.

12. Join lines in the same header, paragraph, etc.

13. Output a clean, continuous version of the document, with clear paragraph breaks and titles where appropriate.

Work carefully and ensure no important content is accidentally removed. Do not translate or rewrite the content. Keep the Tatar text unchanged. Your task is to improve structure and readability only. The language is Tatar, written in Cyrillic.

---- End of instruction -----
"""
