from monocorpus_models import Session, Document
from sqlalchemy import text, select
import re
import time
from rich.syntax import Syntax
from rich.console import Console

# python3 src/main.py select "SELECT COUNT(*) FROM DOCUMENT WHERE metadata_json IS NOT NULL"
# SELECT COUNT(*) FROM DOCUMENT WHERE metadata_json IS NOT NULL
# SELECT COUNT(*) FROM Document
# SELECT mime_type, COUNT(*) AS count, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM Document), 2) AS percent FROM Document GROUP BY mime_type ORDER BY percent DESC
# SELECT language, COUNT(*) AS count, ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM Document), 2) AS percent FROM Document GROUP BY language ORDER BY percent DESC

def sheets_introspect(query):
    start = time.time()
    with Session() as s, Console() as console:
        console.print(f'Executing query:')
        console.print(Syntax(query, "sql", theme="monokai"))
        
        query = query.strip()
        query = re.sub(r'from document', f"FROM '{Document.__tablename__}'", query, flags=re.IGNORECASE)
        query = re.sub(r'^select ', "", query, flags=re.IGNORECASE)
        
        res = s._get_session().execute(select(text(query)))
        console.print("Result:")
        console.print("=" * 10)
        for r in res:
            console.print(r)
        console.print("=" * 10)
    end = time.time()
    console.print(f"Execution time => {round(end - start, 1)} sec")
