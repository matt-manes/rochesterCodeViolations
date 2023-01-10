import json
from datetime import datetime
from pathlib import Path

from dataBased import DataBased, dataToString

root = Path(__file__).parent


""" Taking the data from CodeViolationsOriginal.json and
saving it into a couple different formats that are easier to look at
after trimming it up."""


dbPath = root / "rochesterCodeViolations.db"


def dataBaseInit() -> list[str]:
    """Initialize the database
    and return the list of column
    names in their new order."""
    with DataBased(dbPath) as db:
        columnDefs = ", ".join(
            [
                "OBJECTID int unique",
                "Name text",
                "ADDRESS text",
                "OWNER text",
                "OPEN_CASE text",
                "OPEN_CASES int",
                "VACANT text",
                "OUTSTANDING_VIOLATIONS int",
                "CODE_ENFORCEMENT text",
                "TOTAL_TICKETS int",
                "PENDING_FINES text",
                "FINE_BALANCE text",
                "TAX_FORECLOSURE text",
                "DEMO_CASE text",
                "PERMIT_CASE text",
                "CASE_OPEN_DATE timestamp",
                "LAST_TICKET_DATE timestamp",
                "VACANT_DATE timestamp",
                "DOW text",
                "USE_CODE text",
                "SBL text",
            ]
        )
        db.createTables([f"violations({columnDefs})"])
        return db.getColumnNames("violations")


def reorder(violation: dict, newOrder: list[str]) -> dict:
    """Reorder a violation dict."""
    return {key: violation[key] for key in newOrder}


def process(violation: dict) -> dict:
    # Many entries have big ugly gaps in the address field
    violation["ADDRESS"] = " ".join(chunk for chunk in violation["ADDRESS"].split())
    # Convert date strings to datetime objects if it isn't null
    for date in ["CASE_OPEN_DATE", "LAST_TICKET_DATE", "VACANT_DATE"]:
        if violation[date]:
            violation[date] = datetime.strptime(violation[date], "%Y-%m-%d")
    return violation


if __name__ == "__main__":
    newOrder = dataBaseInit()
    violations = json.loads((root / "codeViolationsOriginal.json").read_text())[
        "features"
    ]
    violations = [
        reorder(process(violation["properties"]), newOrder) for violation in violations
    ]
    # Put the violations in the database
    with DataBased(dbPath) as db:
        for violation in violations:
            db.addToTable("violations", tuple(violation.values()))

    # Put the trimmed up data in a new json file
    (root / "rochesterCodeViolations.json").write_text(
        json.dumps(violations, indent=2, default=str)
    )

    # Put the violations in a grid and dump to txt file
    (root / "rochesterCodeViolations.txt").write_text(
        dataToString(violations, wrapToTerminal=False)
    )
