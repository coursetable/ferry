import csv
from io import StringIO

import pandas as pd
from sqlalchemy import text

from ferry.database import AcademicCalendarEvent, Database


def sync_db_academic_calendar(events: pd.DataFrame, database_connect_string: str):
    """
    Replace the academic calendar table with the given events. The table is
    rebuilt from all parsed calendars every time, so dropping and recreating it
    within one transaction is simpler than diffing and never leaves it empty.
    """
    db = Database(database_connect_string)
    table = AcademicCalendarEvent.__table__

    print("\nReplacing academic calendar table...")
    with db.Engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table.name} CASCADE;"))
        table.create(conn)

        buffer = StringIO()
        events.replace({r"\r": ""}, regex=True).to_csv(
            buffer,
            header=False,
            index=False,
            sep="\t",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            na_rep="NULL",
        )
        buffer.seek(0)
        cursor = conn.connection.cursor()
        cursor.copy_from(
            buffer, table.name, columns=events.columns, sep="\t", null="NULL"
        )
        cursor.close()

        # Keep the id sequence in step with the copied ids
        conn.execute(
            text(
                f"SELECT setval(pg_get_serial_sequence('{table.name}', 'id'), "
                + f"COALESCE((SELECT MAX(id) FROM {table.name}), 0) + 1, false);"
            )
        )

        conn.execute(
            text(
                f"""
DO
$do$
BEGIN
IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'hasura') THEN
    CREATE USER hasura;
END IF;
GRANT SELECT ON {table.name} TO hasura;
END
$do$;
        """
            )
        )
    print("\033[F", end="")
    print("Replacing academic calendar table... ✔")
    print(f"{table.name:>25} - {len(events):6} rows")
